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

/**
 * A query that matches documents based on a wildcard pattern in a single column.
 * This query handles both:
 * 1. Exact matches: terms that exactly match the search value
 * 2. Wildcard matches: terms ending with "*" that are prefixes of the search value
 *
 * All matches are combined using OR logic (SHOULD clause).
 */
public class WildcardMatchSingleColumnQuery extends Query {
    private static final BytesRef STAR_PATTERN = new BytesRef("*");

    private final String searchValue;
    private final MappedFieldType folderField;
    private final SearchExecutionContext context;

    public WildcardMatchSingleColumnQuery(String searchValue, MappedFieldType folderField, SearchExecutionContext context) {
        this.searchValue = searchValue;
        this.folderField = folderField;
        this.context = context;
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        if (searchValue == null || searchValue.isEmpty()) {
            return new MatchNoDocsQuery("searchValue is null or empty");
        }

        // Get all matching terms (exact and prefix) in a single Lucene pass
        Set<BytesRef> matchingTerms = findMatchingTerms(searcher.getIndexReader(), searchValue, folderField);

        // Build query from all matching terms
        return buildQueryFromTerms(matchingTerms);
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
                // Process both exact and wildcard term matches in a single unified method
                totalMatches += processMatchingTerms(
                    leafContext,
                    folderField.name(),
                    searchBytes,
                    docsBuilder,
                    segmentsBuilder,
                    positionsBuilder,
                    position
                );
            }
            throw new UnsupportedOperationException("This approach is still under development");
            // return totalMatches;
        } catch (Exception e) {
            if (warnings != null) {
                warnings.registerException(e);
            }
            return 0;
        }
    }

    /**
     * Builds a Query from matching terms.
     * This method is shared between rewrite() and processQuery().
     */
    private Query buildQueryFromTerms(Set<BytesRef> matchingTerms) throws IOException {
        if (matchingTerms.isEmpty()) {
            return new MatchNoDocsQuery("No matching terms found in folder field");
        }

        return folderField.termsQuery(matchingTerms, context);
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

    /**
     * Processes both exact and wildcard term matches for a single leaf reader context.
     * Finds matching terms and immediately collects document IDs using PostingsEnum.
     * This unified method handles both exact matches and wildcard patterns (terms ending with *) in one pass.
     */
    private int processMatchingTerms(
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

        int totalMatches = 0;
        // Initialize counter for wildcard matching
        int[] wildcardMatchesCounter = new int[1];

        // Step 1: Check for exact match (non-wildcard term that exactly matches searchValue)
        // Note: seekExact only finds exact matches, so if searchValue is "abc", it won't find "abc*"
        // If we find a term that ends with *, it means the term IS searchValue and ends with *
        // In that case, we should process it as a wildcard match
        if (termsEnum.seekExact(searchBytes)) {
            BytesRef foundTerm = termsEnum.term();
            if (foundTerm != null) {
                // Check if this term ends with * - if so, it's a wildcard pattern
                if (foundTerm.length > 0 && foundTerm.bytes[foundTerm.offset + foundTerm.length - 1] == '*') {
                    // This is a wildcard pattern that exactly matches searchValue - process it immediately
                    PostingsEnum postings = termsEnum.postings(null, 0);
                    int count = collectDocumentIds(postings, leafContext, docsBuilder, segmentsBuilder, positionsBuilder, position);
                    wildcardMatchesCounter[0] += count;
                } else {
                    // This is a true exact match (not a wildcard pattern) - collect document IDs
                    PostingsEnum postings = termsEnum.postings(null, 0);
                    totalMatches += collectDocumentIds(postings, leafContext, docsBuilder, segmentsBuilder, positionsBuilder, position);
                }
            }
        }

        // Step 2: Find wildcard matching terms using the unified handler method
        findMatchingPrefixTermsWithHandler(termsEnum, searchBytes, (termsEnumParam, matchingTerm, counter) -> {
            PostingsEnum postings = termsEnumParam.postings(null, 0);
            int count = collectDocumentIds(postings, leafContext, docsBuilder, segmentsBuilder, positionsBuilder, position);
            if (counter != null) {
                counter[0] += count;
            }
        }, wildcardMatchesCounter);

        totalMatches += wildcardMatchesCounter[0];

        return totalMatches;
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
    }

    @Override
    public String toString(String field) {
        return "WildcardMatchSingleColumnQuery(searchValue=" + searchValue + ", folderField=" + folderField.name() + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        WildcardMatchSingleColumnQuery that = (WildcardMatchSingleColumnQuery) obj;
        return Objects.equals(searchValue, that.searchValue) && Objects.equals(folderField, that.folderField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(searchValue, folderField);
    }

    public String getSearchValue() {
        return searchValue;
    }

    public MappedFieldType getFolderField() {
        return folderField;
    }

    /**
     * Finds all matching terms (exact and prefix) in a single Lucene pass.
     * This optimizes rewrite() by avoiding duplicate Terms/TermsEnum lookups.
     */
    private Set<BytesRef> findMatchingTerms(IndexReader indexReader, String searchValue, MappedFieldType folderField) throws IOException {
        Set<BytesRef> matchingTerms = new HashSet<>();

        if (folderField == null || searchValue == null || searchValue.isEmpty()) {
            return matchingTerms;
        }

        BytesRef searchBytes = new BytesRef(searchValue);

        // Get terms for the folder field once
        Terms terms = MultiTerms.getTerms(indexReader, folderField.name());
        if (terms == null) {
            return matchingTerms;
        }

        TermsEnum termsEnum = terms.iterator();
        if (termsEnum == null) {
            return matchingTerms;
        }

        // Add searchValue as exact match (query building will handle non-existent terms)
        matchingTerms.add(searchBytes);

        // Find prefix matching terms (wildcard patterns)
        findMatchingPrefixTermsWithHandler(termsEnum, searchBytes, (termsEnumParam, matchingTerm, counter) -> {
            matchingTerms.add(BytesRef.deepCopyOf(matchingTerm));
            // Counter not needed when just collecting terms
        }, null);

        return matchingTerms;
    }

    /**
     * Handler interface for processing matching wildcard terms.
     * The handler can optionally update a counter to track the number of items processed.
     */
    @FunctionalInterface
    private interface WildcardMatchHandler {
        /**
         * Process a matching wildcard term.
         * @param termsEnum The TermsEnum positioned at the matching term
         * @param matchingTerm The matching term
         * @param counter Optional counter to update with the number of items processed (can be null if not needed)
         * @throws IOException if an I/O error occurs
         */
        void handleMatch(TermsEnum termsEnum, BytesRef matchingTerm, int[] counter) throws IOException;
    }

    /**
     * Core algorithm to find matching prefix terms (terms ending with *) and process them using a handler.
     *
     * Algorithm: Prefix Search using seekCeil with skipping based on common prefix length.
     *
     * We need to find all terms in the folder field that:
     * 1. End with "*" (wildcard patterns)
     * 2. When the "*" is removed, the remaining prefix is a prefix of the search value
     *
     * Strategy:
     * - Start with a prefix of length 1 from the search value
     * - For each prefix length:
     *   a. seekCeil(prefix) finds the next term >= prefix in the dictionary
     *   b. Compute the common prefix length between the found term (without *) and the search value
     *   c. Verify a match: Check if the found term ends with * and its prefix (without *) is a prefix of the search value
     *      - If yes, call the handler
     *   d. Skip unmatching prefix: Use the common prefix length to advance the search
     *      - Because the common prefix length is N, we know there is no point in searching
     *        for prefixes of length 2, 3, ..., N (since the dictionary has already shown what
     *        the next term is). We can advance our search to a prefix of length N+1.
     *   e. Construct the next prefix to search for (increment length based on common prefix)
     *   f. Continue until we reach the upperBound
     *
     * @param termsEnum The TermsEnum to search through
     * @param searchBytes The search value as BytesRef
     * @param handler Handler to process matching terms
     * @param counter Optional counter array to track total count (can be null if not needed)
     */
    private void findMatchingPrefixTermsWithHandler(TermsEnum termsEnum, BytesRef searchBytes, WildcardMatchHandler handler, int[] counter)
        throws IOException {
        // Always check for "*" pattern (matches everything)
        TermsEnum.SeekStatus starSeekStatus = termsEnum.seekCeil(STAR_PATTERN);
        if (starSeekStatus == TermsEnum.SeekStatus.FOUND) {
            BytesRef foundTerm = termsEnum.term();
            if (foundTerm != null && foundTerm.length == 1 && foundTerm.bytes[foundTerm.offset] == '*') {
                handler.handleMatch(termsEnum, foundTerm, counter);
            }
        }

        // Step 1: Upper bound - terms greater than searchValue + "*" cannot match
        // Build searchValue + "*" efficiently without string conversion
        byte[] searchBytesArray = new byte[searchBytes.length + 1];
        System.arraycopy(searchBytes.bytes, searchBytes.offset, searchBytesArray, 0, searchBytes.length);
        searchBytesArray[searchBytes.length] = (byte) '*';
        BytesRef searchValueWithStar = new BytesRef(searchBytesArray);
        BytesRef upperBound = null;
        TermsEnum.SeekStatus upperBoundStatus = termsEnum.seekCeil(searchValueWithStar);
        if (upperBoundStatus == TermsEnum.SeekStatus.NOT_FOUND) {
            // Found a term greater than searchValue + "*" - this is our upper bound
            BytesRef currentTerm = termsEnum.term();
            if (currentTerm != null) {
                upperBound = BytesRef.deepCopyOf(currentTerm);
            }
        } else if (upperBoundStatus == TermsEnum.SeekStatus.FOUND) {
            // Found exact match: searchValue + "*" exists
            BytesRef exactMatchTerm = termsEnum.term();
            if (exactMatchTerm != null) {
                handler.handleMatch(termsEnum, exactMatchTerm, counter);
            }
            // Move to next term for upper bound
            if (termsEnum.next() != null) {
                BytesRef nextTerm = termsEnum.term();
                if (nextTerm != null) {
                    upperBound = BytesRef.deepCopyOf(nextTerm);
                }
            }
        }
        // If upperBoundStatus == END, upperBound remains null (no upper bound, check all terms)

        // Step 2: Iterate and Match using smart prefix jumping
        // Start with a prefix of length 1
        int prefixLength = 1;

        BytesRef prefix = new BytesRef(searchBytes.bytes, searchBytes.offset, prefixLength);
        // Reusable BytesRef for prefix + "*"
        byte[] prefixWithStarArray = new byte[searchBytes.length + 1];
        BytesRef prefixWithStar = new BytesRef(prefixWithStarArray);

        while (prefixLength <= searchBytes.length) {
            // Update the length of the reusable BytesRef to represent the current prefix length
            prefix.length = prefixLength;

            // Check if we've reached the upper bound (prefix >= upperBound means we're done)
            // Optimized: use direct byte comparison instead of compareTo
            if (upperBound != null && isGreaterOrEqual(prefix, prefixLength, upperBound)) {
                break;
            }

            // Build prefix + "*" to search for wildcard terms
            System.arraycopy(searchBytes.bytes, searchBytes.offset, prefixWithStarArray, 0, prefixLength);
            prefixWithStarArray[prefixLength] = (byte) '*';
            prefixWithStar.length = prefixLength + 1;

            // seekCeil(prefix + "*") finds the next wildcard term >= prefix + "*"
            TermsEnum.SeekStatus seekStatus = termsEnum.seekCeil(prefixWithStar);

            if (seekStatus == TermsEnum.SeekStatus.END) {
                // No more terms in dictionary
                break;
            }

            BytesRef foundTerm = termsEnum.term();
            if (foundTerm == null) {
                break;
            }

            // Check if we've reached the upper bound
            if (upperBound != null && isGreaterOrEqual(foundTerm, foundTerm.length, upperBound)) {
                break;
            }

            // Check if this term starts with the prefix and is a wildcard
            if (foundTerm.length >= prefixLength
                && prefixMatches(foundTerm, prefix, prefixLength)
                && foundTerm.bytes[foundTerm.offset + foundTerm.length - 1] == '*') {

                // Process this wildcard term
                int wildcardPrefixLength = foundTerm.length - 1;

                // Compute common prefix length and check match in one pass
                int commonPrefixLength = computeCommonPrefixLength(foundTerm, wildcardPrefixLength, searchBytes);
                boolean isMatch = commonPrefixLength == wildcardPrefixLength && wildcardPrefixLength <= searchBytes.length;

                if (isMatch) {
                    // Match found - process via handler
                    handler.handleMatch(termsEnum, foundTerm, counter);
                    // When matched, advance to foundTerm.length (the * at the end doesn't count for prefix length)
                    // This ensures we continue checking longer prefixes
                    prefixLength = foundTerm.length;
                } else {
                    // Not a match, advance based on common prefix length
                    prefixLength = commonPrefixLength + 1;
                }
            } else {
                int commonPrefixLength = computeCommonPrefixLength(foundTerm, searchBytes);
                prefixLength = Math.max(commonPrefixLength, prefixLength + 1);
            }

            // Check if we've exceeded searchBytes length
            if (prefixLength > searchBytes.length) {
                break;
            }
        }
    }

    /**
     * Checks if the first length bytes of a BytesRef match another BytesRef.
     * Returns true if they match, false otherwise.
     */
    private static boolean prefixMatches(BytesRef term, BytesRef prefix, int length) {
        if (term.length < length || prefix.length < length) {
            return false;
        }
        for (int i = 0; i < length; i++) {
            if (term.bytes[term.offset + i] != prefix.bytes[prefix.offset + i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Optimized comparison: checks if a BytesRef (with given length) is >= upperBound.
     * This is faster than compareTo because we can stop early and avoid full comparison.
     * Returns true if term >= upperBound, false otherwise.
     */
    private static boolean isGreaterOrEqual(BytesRef term, int termLength, BytesRef upperBound) {
        int minLength = Math.min(termLength, upperBound.length);
        for (int i = 0; i < minLength; i++) {
            byte termByte = term.bytes[term.offset + i];
            byte upperByte = upperBound.bytes[upperBound.offset + i];
            if (termByte != upperByte) {
                // Only convert to unsigned when bytes differ
                return (termByte & 0xFF) > (upperByte & 0xFF);
            }
        }
        // All bytes match up to minLength - compare lengths
        return termLength >= upperBound.length;
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
     * Computes the common prefix length between a BytesRef (considering only up to maxLength bytes)
     * and another BytesRef. This avoids creating intermediate BytesRef objects.
     */
    private int computeCommonPrefixLength(BytesRef term1, int maxLength, BytesRef term2) {
        int minLength = Math.min(maxLength, term2.length);
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
        // Fast path: if lengths are equal, do a simple comparison
        if (term1.length == term2.length) {
            return term1.equals(term2);
        }
        // term1.length is already checked to be <= term2.length by caller
        // Compare byte-by-byte
        for (int i = 0; i < term1.length; i++) {
            if (term1.bytes[term1.offset + i] != term2.bytes[term2.offset + i]) {
                return false;
            }
        }
        return true;
    }

}
