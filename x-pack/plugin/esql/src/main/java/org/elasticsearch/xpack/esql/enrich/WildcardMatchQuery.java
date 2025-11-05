/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * A query that matches documents based on a wildcard pattern.
 * This query combines:
 * 1. Exact term matching on the term field (for exact matches)
 * 2. Prefix wildcard matching on the wildcard field (for prefix matches)
 *
 * The two results are combined using OR logic (SHOULD clause).
 */
public class WildcardMatchQuery extends Query {
    private final String searchValue;
    private final MappedFieldType termField;
    private final MappedFieldType wildcardField;
    private final SearchExecutionContext context;

    public WildcardMatchQuery(
        String searchValue,
        MappedFieldType termField,
        MappedFieldType wildcardField,
        SearchExecutionContext context
    ) {
        this.searchValue = searchValue;
        this.termField = termField;
        this.wildcardField = wildcardField;
        this.context = context;
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        if (searchValue == null || searchValue.isEmpty()) {
            return new MatchNoDocsQuery("searchValue is null or empty");
        }

        // Step 1: Get exact matching term from termField
        BytesRef exactTerm = findExactMatchingTerms(searcher, searchValue, termField);

        // Step 2: Get prefix matching terms from wildcardField
        Set<BytesRef> prefixTerms = findMatchingPrefixTerms(searcher, searchValue, wildcardField);

        // Step 3: Build one query for both
        return buildQueryFromTerms(exactTerm, prefixTerms);
    }

    /**
     * Process a single query at the given position using direct Lucene index access.
     * This method bypasses Lucene's query framework entirely and directly accesses
     * the inverted index using TermsEnum and PostingsEnum for maximum performance.
     * Optimized to collect document IDs while finding matching terms, avoiding a second pass.
     */
    public int processQuery(
        String searchValue,
        IndexReader indexReader,
        IntVector.Builder docsBuilder,
        IntVector.Builder segmentsBuilder,
        IntVector.Builder positionsBuilder,
        int position,
        Warnings warnings
    ) {
        try {
            if (searchValue == null || searchValue.isEmpty()) {
                return 0;
            }

            BytesRef searchBytes = new BytesRef(searchValue);
            int totalMatches = 0;

            // Process each segment separately, finding terms and collecting document IDs in one pass
            for (LeafReaderContext leafContext : indexReader.leaves()) {
                // Process exact term matches from termField
                if (termField != null) {
                    totalMatches += processExactTermMatches(
                        leafContext,
                        termField.name(),
                        searchBytes,
                        docsBuilder,
                        segmentsBuilder,
                        positionsBuilder,
                        position
                    );
                }

                // Process prefix term matches from wildcardField
                if (wildcardField != null) {
                    totalMatches += processPrefixTermMatches(
                        leafContext,
                        wildcardField.name(),
                        searchBytes,
                        docsBuilder,
                        segmentsBuilder,
                        positionsBuilder,
                        position
                    );
                }
            }

            return totalMatches;
        } catch (Exception e) {
            if (warnings != null) {
                warnings.registerException(e);
            }
            return 0;
        }
    }

    /**
     * Builds a Query from the exact term and prefix terms.
     * This method is shared between rewrite() and processQuery().
     */
    private Query buildQueryFromTerms(BytesRef exactTerm, Set<BytesRef> prefixTerms) throws IOException {
        if (exactTerm == null && prefixTerms.isEmpty()) {
            return new MatchNoDocsQuery("No matching terms found in either exact or prefix fields");
        }

        // If only one field has matches, return the query directly without BooleanQuery wrapper
        if (exactTerm == null) {
            return wildcardField.termsQuery(prefixTerms, context);
        }

        if (prefixTerms.isEmpty()) {
            return termField.termQuery(exactTerm, context);
        }

        // Both fields have matches - build a BooleanQuery that queries both fields with their respective terms
        BooleanQuery.Builder builder = new BooleanQuery.Builder();

        builder.add(termField.termQuery(exactTerm, context), BooleanClause.Occur.SHOULD);
        builder.add(wildcardField.termsQuery(prefixTerms, context), BooleanClause.Occur.SHOULD);

        // Combine with OR logic: at least one of the queries must match
        builder.setMinimumNumberShouldMatch(1);
        return builder.build();
    }

    /**
     * Processes exact term matches for a single leaf reader context.
     * Finds the exact term and immediately collects document IDs using PostingsEnum.
     */
    private int processExactTermMatches(
        LeafReaderContext leafContext,
        String fieldName,
        BytesRef searchBytes,
        IntVector.Builder docsBuilder,
        IntVector.Builder segmentsBuilder,
        IntVector.Builder positionsBuilder,
        int position
    ) throws IOException {
        Terms terms = leafContext.reader().terms(fieldName);
        if (terms == null) {
            return 0;
        }
        TermsEnum termsEnum = terms.iterator();
        if (termsEnum == null) {
            return 0;
        }
        if (termsEnum.seekExact(searchBytes) == false) {
            return 0; // Term doesn't exist in this segment
        }
        // Immediately collect document IDs using PostingsEnum
        PostingsEnum postings = termsEnum.postings(null, 0);
        return collectDocumentIds(postings, leafContext, docsBuilder, segmentsBuilder, positionsBuilder, position);
    }

    /**
     * Processes prefix term matches for a single leaf reader context.
     * Finds matching prefix terms and immediately collects document IDs using PostingsEnum.
     * This avoids storing terms and then iterating through them again.
     */
    private int processPrefixTermMatches(
        LeafReaderContext leafContext,
        String fieldName,
        BytesRef searchBytes,
        IntVector.Builder docsBuilder,
        IntVector.Builder segmentsBuilder,
        IntVector.Builder positionsBuilder,
        int position
    ) throws IOException {
        Terms terms = leafContext.reader().terms(fieldName);
        if (terms == null) {
            return 0;
        }
        TermsEnum termsEnum = terms.iterator();
        if (termsEnum == null) {
            return 0;
        }

        int[] totalMatches = new int[1]; // Use array to allow modification in lambda
        Set<BytesRef> processedTerms = new HashSet<>(); // Track processed terms to avoid duplicates

        // Use the shared algorithm to find matching prefix terms, collecting document IDs immediately
        findMatchingPrefixTermsWithHandler(termsEnum, searchBytes, (foundTerm, enumRef) -> {
            // Handler: immediately collect document IDs for matching terms
            BytesRef termCopy = BytesRef.deepCopyOf(foundTerm);
            if (processedTerms.add(termCopy)) {
                try {
                    PostingsEnum postings = enumRef.postings(null, 0);
                    totalMatches[0] += collectDocumentIds(postings, leafContext, docsBuilder, segmentsBuilder, positionsBuilder, position);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        return totalMatches[0];
    }

    /**
     * Collects document IDs from PostingsEnum and appends them to the builders.
     * This helper method is shared between exact and prefix term processing.
     */
    private int collectDocumentIds(
        PostingsEnum postings,
        LeafReaderContext leafContext,
        IntVector.Builder docsBuilder,
        IntVector.Builder segmentsBuilder,
        IntVector.Builder positionsBuilder,
        int position
    ) throws IOException {
        int docId;
        int matches = 0;
        while ((docId = postings.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
            docsBuilder.appendInt(docId);
            if (segmentsBuilder != null) {
                segmentsBuilder.appendInt(leafContext.ord);
            }
            positionsBuilder.appendInt(position);
            matches++;
        }
        return matches;
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
    }

    @Override
    public String toString(String field) {
        return "WildcardMatchQuery(searchValue="
            + searchValue
            + ", termField="
            + termField.name()
            + ", wildcardField="
            + wildcardField.name()
            + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        WildcardMatchQuery that = (WildcardMatchQuery) obj;
        return Objects.equals(searchValue, that.searchValue)
            && Objects.equals(termField, that.termField)
            && Objects.equals(wildcardField, that.wildcardField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(searchValue, termField, wildcardField);
    }

    public String getSearchValue() {
        return searchValue;
    }

    public MappedFieldType getTermField() {
        return termField;
    }

    public MappedFieldType getWildcardField() {
        return wildcardField;
    }

    /**
     * Finds the exact matching term in the term field.
     * Returns the exact term if it exists, otherwise null.
     * This method is shared between rewrite() and processQuery().
     */
    private BytesRef findExactMatchingTerms(IndexSearcher searcher, String searchValue, MappedFieldType termField) throws IOException {
        return findExactMatchingTerms(searcher.getIndexReader(), searchValue, termField);
    }

    /**
     * Finds the exact matching term in the term field using IndexReader.
     * Returns the exact term if it exists, otherwise null.
     * This method is shared between rewrite() and processQuery().
     */
    private BytesRef findExactMatchingTerms(IndexReader indexReader, String searchValue, MappedFieldType termField) throws IOException {
        if (termField == null || searchValue == null || searchValue.isEmpty()) {
            return null;
        }

        BytesRef searchBytes = new BytesRef(searchValue);

        // Get terms for the term field
        Terms terms = MultiTerms.getTerms(indexReader, termField.name());
        if (terms == null) {
            return null;
        }

        TermsEnum termsEnum = terms.iterator();
        if (termsEnum == null) {
            return null;
        }

        // Use seekExact to check if the exact term exists
        if (termsEnum.seekExact(searchBytes)) {
            BytesRef foundTerm = termsEnum.term();
            if (foundTerm != null) {
                return BytesRef.deepCopyOf(foundTerm);
            }
        }

        return null;
    }

    /**
     * Finds all terms in the prefix field that are prefixes of the search value.
     *
     * Algorithm: Prefix Search using seekCeil
     *
     * Step 1: Upper Bound
     * - Use seekCeil(searchValue) on the Prefix Terms Field to find the first term >= searchValue.
     * - This term is the upperBound, a termination condition.
     *
     * Step 2: Iterate and Match
     * - Start with a prefix of length 1.
     * - For each prefix length:
     *   a. seekCeil(prefix) finds the next term >= prefix in the dictionary.
     *   b. Compute the common prefix length between the found term and the query term.
     *   c. Verify a match: Check if the found term is a prefix of the query term.
     *      - If yes, add it to results.
     *   d. Skip unmatching prefix: Use the common prefix length to advance the search.
     *      - Because the common prefix length is N, we know there is no point in searching
     *        for prefixes of length 2, 3, ..., N (since the dictionary has already shown what
     *        the next term is). We can advance our search to a prefix of length N+1.
     *   e. Construct the next prefix to search for (increment length based on common prefix).
     *   f. Continue until we reach the upperBound.
     * This method is shared between rewrite() and processQuery().
     */
    private Set<BytesRef> findMatchingPrefixTerms(IndexSearcher searcher, String searchValue, MappedFieldType prefixField)
        throws IOException {
        return findMatchingPrefixTerms(searcher.getIndexReader(), searchValue, prefixField);
    }

    /**
     * Finds all terms in the prefix field that are prefixes of the search value using IndexReader.
     * This method is shared between rewrite() and processQuery().
     */
    private Set<BytesRef> findMatchingPrefixTerms(IndexReader indexReader, String searchValue, MappedFieldType prefixField)
        throws IOException {
        Set<BytesRef> matchingTermsSet = new HashSet<>();

        if (searchValue == null || searchValue.isEmpty()) {
            return matchingTermsSet;
        }

        BytesRef searchBytes = new BytesRef(searchValue);

        // Get terms for the prefix field
        Terms terms = MultiTerms.getTerms(indexReader, prefixField.name());
        if (terms == null) {
            return matchingTermsSet;
        }

        TermsEnum termsEnum = terms.iterator();
        if (termsEnum == null) {
            return matchingTermsSet;
        }

        // Use the shared algorithm to find matching prefix terms, collecting them into a Set
        findMatchingPrefixTermsWithHandler(termsEnum, searchBytes, (foundTerm, enumRef) -> {
            // Handler: add matching term to the set
            matchingTermsSet.add(BytesRef.deepCopyOf(foundTerm));
        });

        return matchingTermsSet;
    }

    /**
     * Core algorithm to find matching prefix terms.
     * This shared method implements the prefix matching algorithm and accepts a handler
     * to process each matching term (either collect into a Set or immediately process with PostingsEnum).
     *
     * Algorithm: Prefix Search using seekCeil
     *
     * Step 1: Upper Bound
     * - Use seekCeil(searchValue) on the Prefix Terms Field to find the first term >= searchValue.
     * - This term is the upperBound, a termination condition.
     *
     * Step 2: Iterate and Match
     * - Start with a prefix of length 1.
     * - For each prefix length:
     *   a. seekCeil(prefix) finds the next term >= prefix in the dictionary.
     *   b. Compute the common prefix length between the found term and the query term.
     *   c. Verify a match: Check if the found term is a prefix of the query term.
     *      - If yes, call the handler.
     *   d. Skip unmatching prefix: Use the common prefix length to advance the search.
     *      - Because the common prefix length is N, we know there is no point in searching
     *        for prefixes of length 2, 3, ..., N (since the dictionary has already shown what
     *        the next term is). We can advance our search to a prefix of length N+1.
     *   e. Construct the next prefix to search for (increment length based on common prefix).
     *   f. Continue until we reach the upperBound.
     *
     * @param termsEnum The TermsEnum to search through
     * @param searchBytes The search value as BytesRef
     * @param matchHandler Handler called for each matching term: (foundTerm, termsEnum) -> void
     */
    private void findMatchingPrefixTermsWithHandler(TermsEnum termsEnum, BytesRef searchBytes, BiConsumer<BytesRef, TermsEnum> matchHandler)
        throws IOException {
        // Always check for empty prefix "" (represents "*" pattern which matches everything)
        // An empty prefix is a prefix of any non-empty search value, so if it exists, include it
        BytesRef emptyPrefix = new BytesRef("");
        TermsEnum.SeekStatus emptySeekStatus = termsEnum.seekCeil(emptyPrefix);
        if (emptySeekStatus == TermsEnum.SeekStatus.FOUND) {
            BytesRef foundTerm = termsEnum.term();
            if (foundTerm != null && foundTerm.length == 0) {
                matchHandler.accept(foundTerm, termsEnum);
            }
        }

        // Step 1: Upper Bound - Use seekCeil(searchValue) to find the first term >= searchValue
        // This term is the upperBound, a termination condition.
        BytesRef upperBound = null;
        boolean foundExactMatch = false;
        TermsEnum.SeekStatus seekStatus = termsEnum.seekCeil(searchBytes);
        if (seekStatus == TermsEnum.SeekStatus.FOUND) {
            // Exact match found - this term equals the search value, so it's a prefix of itself
            BytesRef exactMatchTerm = termsEnum.term();
            if (exactMatchTerm != null) {
                matchHandler.accept(exactMatchTerm, termsEnum);
                foundExactMatch = true;
            }
            // Move to next term for upper bound
            if (termsEnum.next() != null) {
                BytesRef nextTerm = termsEnum.term();
                if (nextTerm != null) {
                    upperBound = BytesRef.deepCopyOf(nextTerm);
                }
            }
        } else if (seekStatus == TermsEnum.SeekStatus.NOT_FOUND) {
            // Found a term greater than searchValue - this is our upper bound
            BytesRef currentTerm = termsEnum.term();
            if (currentTerm != null) {
                upperBound = BytesRef.deepCopyOf(currentTerm);
            }
        }
        // If seekStatus == END, upperBound remains null (no upper bound, check all terms)

        // Step 2: Iterate and Match
        // Start with a prefix of length 1
        // Optimization: If we found an exact match and it equals the full search value,
        // we can skip the iteration when prefixLength equals searchBytes.length to avoid re-checking
        int prefixLength = 1;

        // Optimization: Reuse a single BytesRef object instead of creating a new one each iteration
        // We'll update its length field to represent different prefix lengths
        BytesRef prefix = new BytesRef(searchBytes.bytes, searchBytes.offset, prefixLength);

        while (prefixLength <= searchBytes.length) {
            // Optimization: If we found an exact match and we're at the full length,
            // skip to avoid re-checking the same term (Set handles duplicates, but this avoids work)
            if (foundExactMatch && prefixLength == searchBytes.length) {
                break;
            }

            // Update the length of the reusable BytesRef to represent the current prefix length
            prefix.length = prefixLength;

            // Check if we've reached the upper bound (prefix >= upperBound means we're done)
            if (upperBound != null && prefix.compareTo(upperBound) >= 0) {
                break;
            }

            // seekCeil(prefix) finds the next term >= prefix in the dictionary
            seekStatus = termsEnum.seekCeil(prefix);

            if (seekStatus == TermsEnum.SeekStatus.END) {
                // No more terms in dictionary
                break;
            }

            BytesRef foundTerm = termsEnum.term();
            if (foundTerm == null) {
                break;
            }

            // Check if we've reached the upper bound
            if (upperBound != null && foundTerm.compareTo(upperBound) >= 0) {
                break;
            }

            // Compute the common prefix length between the found term and the query term
            int commonPrefixLength = computeCommonPrefixLength(foundTerm, searchBytes);

            // Verify a match: Check if the found term is a prefix of the query term
            boolean isMatch = isPrefixOf(foundTerm, searchBytes);
            if (isMatch) {
                // Match found - call the handler
                matchHandler.accept(foundTerm, termsEnum);
            }

            // Skip unmatching prefix: Use the common prefix length to advance the search
            // Because the common prefix length is N, we know there is no point in searching
            // for prefixes of length 2, 3, ..., N (since the dictionary has already shown
            // what the next term is). We can advance our search to a prefix of length N+1.
            // If we found a match, we also need to ensure we advance past the found term's length
            // to avoid checking it again, so we use max(commonPrefixLength + 1, foundTerm.length + 1)
            int nextPrefixLength = commonPrefixLength + 1;
            if (isMatch) {
                // If we found a match, advance past the found term's length
                nextPrefixLength = Math.max(nextPrefixLength, foundTerm.length + 1);
            }
            prefixLength = nextPrefixLength;
        }
    }

    /**
     * Computes the common prefix length between two BytesRef terms.
     * Returns the length of the longest common prefix.
     */
    private int computeCommonPrefixLength(BytesRef term1, BytesRef term2) {
        int minLength = Math.min(term1.length, term2.length);
        for (int i = 0; i < minLength; i++) {
            if (term1.bytes[term1.offset + i] != term2.bytes[term2.offset + i]) {
                return i;
            }
        }
        return minLength;
    }

    /**
     * Checks if term1 is a prefix of term2.
     * A term is a prefix if all its bytes match the corresponding bytes at the start of term2.
     */
    private boolean isPrefixOf(BytesRef term1, BytesRef term2) {
        if (term1.length > term2.length) {
            return false;
        }
        for (int i = 0; i < term1.length; i++) {
            if (term1.bytes[term1.offset + i] != term2.bytes[term2.offset + i]) {
                return false;
            }
        }
        return true;
    }
}
