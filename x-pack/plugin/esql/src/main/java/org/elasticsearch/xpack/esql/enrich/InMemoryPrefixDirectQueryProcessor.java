/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.compute.operator.lookup.DirectQueryProcessor;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * DirectQueryProcessor that loads all lookup documents into memory and filters them
 * based on prefix matching. The "folder" field in the lookup index contains patterns
 * like "fan", "fola*", "fold*", etc., and we match them against folder_path values.
 */
public class InMemoryPrefixDirectQueryProcessor implements DirectQueryProcessor {
    private final MappedFieldType folderField;
    private final BytesRefBlock block;
    private final Warnings warnings;

    // Cached lookup data: indexed by pattern type for fast lookups
    private volatile Map<String, List<LookupDoc>> exactPatternMap = null;  // Exact patterns -> docs
    private volatile PrefixTrie prefixTrie = null;  // Trie for prefix patterns (ending with *) - used for TRIE approach
    private volatile List<LookupDoc> prefixPatternDocs = null;  // List for prefix patterns - used for BASIC approach
    private volatile IndexReader cachedIndexReader = null;  // Cache by IndexReader to avoid reloading
    private final ExpressionQueryList.Approach approach;  // Which approach to use
    private String resolvedFieldName = null;  // Cached resolved field name (fieldName or literalFieldName)
    private final BytesRef reusableBytesRef = new BytesRef();  // Reusable BytesRef to avoid allocations

    public InMemoryPrefixDirectQueryProcessor(
        MappedFieldType folderField,
        Block block,
        ExpressionQueryList.Approach approach,
        Warnings warnings
    ) {
        this.folderField = folderField;
        this.block = (BytesRefBlock) block;
        this.approach = approach;
        this.warnings = warnings;
    }

    /**
     * Loads all documents from the lookup index into memory.
     * This is called for each IndexReader to load the lookup data.
     */
    private void loadLookupDocs(IndexReader indexReader) throws IOException {
        // Check if already loaded for this IndexReader
        IndexReader currentCached = cachedIndexReader;
        if (exactPatternMap != null && currentCached == indexReader) {
            return; // Already loaded
        }

        // Reset resolved field name if IndexReader changed (different index or refresh)
        if (currentCached != indexReader) {
            resolvedFieldName = null;
        }

        // Build indexed structures for fast lookups
        Map<String, List<LookupDoc>> newExactPatternMap = new HashMap<>();
        PrefixTrie newPrefixTrie = approach == ExpressionQueryList.Approach.IN_MEMORY_PREFIX_TRIE ? new PrefixTrie() : null;
        List<LookupDoc> newPrefixPatternDocs = approach == ExpressionQueryList.Approach.IN_MEMORY_PREFIX_BASIC ? new ArrayList<>() : null;
        // Use the field name from MappedFieldType
        // For keyword fields, name() should return the field name as it appears in the index
        String fieldName = folderField.name();
        // Also try the literal name "folder" in case there's a mismatch
        String literalFieldName = "folder";

        // Resolve field name once - check first segment to determine which name works
        if (resolvedFieldName == null) {
            List<LeafReaderContext> leaves = indexReader.leaves();
            for (LeafReaderContext leafContext : leaves) {
                FieldInfos fieldInfos = leafContext.reader().getFieldInfos();
                var fieldInfo = fieldInfos.fieldInfo(fieldName);
                if (fieldInfo != null) {
                    resolvedFieldName = fieldName;
                    break;
                }
                fieldInfo = fieldInfos.fieldInfo(literalFieldName);
                if (fieldInfo != null) {
                    resolvedFieldName = literalFieldName;
                    break;
                }
            }
            // If still not resolved, default to fieldName
            if (resolvedFieldName == null) {
                resolvedFieldName = fieldName;
            }
        }

        // Iterate through all segments
        List<LeafReaderContext> leaves = indexReader.leaves();
        if (leaves.isEmpty()) {
            // No segments in index, nothing to load
            cachedIndexReader = indexReader;
            exactPatternMap = newExactPatternMap;
            if (approach == ExpressionQueryList.Approach.IN_MEMORY_PREFIX_TRIE) {
                prefixTrie = newPrefixTrie;
            } else {
                prefixPatternDocs = newPrefixPatternDocs;
            }
            return;
        }

        for (int segmentIndex = 0; segmentIndex < leaves.size(); segmentIndex++) {
            LeafReaderContext leafContext = leaves.get(segmentIndex);
            var leafReader = leafContext.reader();

            // Check if field exists in this segment's FieldInfos
            FieldInfos fieldInfos = leafReader.getFieldInfos();
            var fieldInfo = fieldInfos.fieldInfo(resolvedFieldName);
            if (fieldInfo == null) {
                // Field doesn't exist in this segment at all
                // This could mean the IndexReader is for a different index
                continue;
            }

            // Check the doc values type
            org.apache.lucene.index.DocValuesType docValuesType = fieldInfo.getDocValuesType();

            if (docValuesType == org.apache.lucene.index.DocValuesType.NONE) {
                // Field exists but doc values are disabled
                // This shouldn't happen for keyword fields by default, but handle it
                continue;
            }

            SortedDocValues folderValues = null;
            SortedSetDocValues folderSetValues = null;
            BinaryDocValues binaryFolderValues = null;
            boolean useBinaryDocValues = false;
            boolean useSortedSetDocValues = false;

            // Try methods based on DocValuesType
            // getSortedDocValues returns null if DocValuesType doesn't match
            if (docValuesType == org.apache.lucene.index.DocValuesType.SORTED) {
                // Single-valued sorted doc values
                folderValues = leafReader.getSortedDocValues(resolvedFieldName);
            } else if (docValuesType == org.apache.lucene.index.DocValuesType.SORTED_SET) {
                // Multi-valued sorted set doc values
                folderSetValues = leafReader.getSortedSetDocValues(resolvedFieldName);
                if (folderSetValues != null) {
                    useSortedSetDocValues = true;
                }
            } else if (docValuesType == org.apache.lucene.index.DocValuesType.BINARY) {
                // Binary doc values
                binaryFolderValues = leafReader.getBinaryDocValues(resolvedFieldName);
                if (binaryFolderValues != null) {
                    useBinaryDocValues = true;
                }
            } else {
                // Unknown type, try all methods as fallback
                folderValues = leafReader.getSortedDocValues(resolvedFieldName);
                if (folderValues == null) {
                    folderSetValues = leafReader.getSortedSetDocValues(resolvedFieldName);
                    if (folderSetValues != null) {
                        useSortedSetDocValues = true;
                    } else {
                        binaryFolderValues = leafReader.getBinaryDocValues(resolvedFieldName);
                        if (binaryFolderValues != null) {
                            useBinaryDocValues = true;
                        }
                    }
                }
            }

            if (folderValues == null && folderSetValues == null && binaryFolderValues == null) {
                // Field exists in FieldInfos but couldn't read doc values
                // This can happen if:
                // 1. No documents in this segment have the field (field exists in mapping but no docs have it)
                // 2. Doc values reader failed for some reason
                // Skip this segment and continue to next
                continue;
            }

            int segmentDocBase = leafContext.docBase;
            int maxDoc = leafReader.maxDoc();
            var liveDocs = leafReader.getLiveDocs();

            // Iterate through all documents in this segment
            for (int localDocId = 0; localDocId < maxDoc; localDocId++) {
                // Skip deleted documents
                if (liveDocs != null && liveDocs.get(localDocId) == false) {
                    continue;
                }

                BytesRef folderBytes = null;
                if (useSortedSetDocValues) {
                    // Use SortedSetDocValues (multi-valued)
                    // For keyword fields, we typically only care about the first value
                    if (folderSetValues.advanceExact(localDocId)) {
                        long ord = folderSetValues.nextOrd();
                        // nextOrd() returns -1 when there are no more ordinals
                        if (ord >= 0) {
                            folderBytes = folderSetValues.lookupOrd(ord);
                        }
                    }
                } else if (useBinaryDocValues) {
                    // Use BinaryDocValues
                    if (binaryFolderValues.advanceExact(localDocId)) {
                        folderBytes = binaryFolderValues.binaryValue();
                    }
                } else {
                    // Use SortedDocValues (single-valued)
                    if (folderValues.advanceExact(localDocId)) {
                        int ord = folderValues.ordValue();
                        int valueCount = folderValues.getValueCount();
                        // ord should be >= 0 and < valueCount
                        if (ord >= 0 && ord < valueCount) {
                            folderBytes = folderValues.lookupOrd(ord);
                        }
                    }
                }

                if (folderBytes != null && folderBytes.length > 0) {
                    String folderPattern = folderBytes.utf8ToString();
                    int globalDocId = segmentDocBase + localDocId;

                    // Index by pattern type for fast lookups
                    int patternLen = folderPattern.length();
                    if (patternLen > 0 && folderPattern.charAt(patternLen - 1) == '*') {
                        // Prefix pattern
                        if (approach == ExpressionQueryList.Approach.IN_MEMORY_PREFIX_TRIE) {
                            // Use Trie approach - store prefix directly (without *)
                            String prefix = patternLen == 1 ? "" : folderPattern.substring(0, patternLen - 1);
                            LookupDoc lookupDoc = new LookupDoc(globalDocId, segmentIndex, localDocId, folderPattern, prefix);
                            newPrefixTrie.insert(prefix, lookupDoc);
                        } else {
                            // Use basic list approach - store prefix to avoid substring later
                            String prefix = patternLen == 1 ? "" : folderPattern.substring(0, patternLen - 1);
                            LookupDoc lookupDoc = new LookupDoc(globalDocId, segmentIndex, localDocId, folderPattern, prefix);
                            newPrefixPatternDocs.add(lookupDoc);
                        }
                    } else {
                        // Exact pattern - add to map
                        LookupDoc lookupDoc = new LookupDoc(globalDocId, segmentIndex, localDocId, folderPattern, null);
                        newExactPatternMap.computeIfAbsent(folderPattern, k -> new ArrayList<>()).add(lookupDoc);
                    }
                }
            }
        }

        // Update cache atomically
        cachedIndexReader = indexReader;
        exactPatternMap = newExactPatternMap;
        if (approach == ExpressionQueryList.Approach.IN_MEMORY_PREFIX_TRIE) {
            prefixTrie = newPrefixTrie;
        } else {
            prefixPatternDocs = newPrefixPatternDocs;
        }
    }

    /**
     * Checks if a folder_path value matches a folder pattern.
     * Patterns can be:
     * - Exact match: "fan" matches "fan"
     * - Prefix match: "fola*" matches "fola1", "fola2", etc.
     */
    private boolean matches(String folderPath, String prefix) {
        if (prefix == null) {
            // This shouldn't happen for prefix patterns, but handle gracefully
            return false;
        }
        if (prefix.isEmpty()) {
            // Empty prefix ("*" pattern) matches everything
            return true;
        }
        // Prefix match: check if folderPath starts with the prefix
        return folderPath.startsWith(prefix);
    }

    @Override
    public int processQuery(
        int position,
        IndexReader indexReader,
        IntVector.Builder docsBuilder,
        IntVector.Builder segmentsBuilder,
        IntVector.Builder positionsBuilder,
        Warnings operatorWarnings
    ) {
        try {
            // Load lookup docs if not already loaded for this IndexReader
            loadLookupDocs(indexReader);

            final int valueCount = block.getValueCount(position);
            if (valueCount != 1) {
                return 0; // Skip multi-value positions and null positions
            }

            final int firstValueIndex = block.getFirstValueIndex(position);
            BytesRef termBytes = block.getBytesRef(firstValueIndex, reusableBytesRef);
            String folderPath = termBytes.utf8ToString();

            // Ensure lookup docs are loaded
            Map<String, List<LookupDoc>> currentExactMap = exactPatternMap;
            PrefixTrie currentPrefixTrie = null;
            List<LookupDoc> currentPrefixDocs = null;

            if (approach == ExpressionQueryList.Approach.IN_MEMORY_PREFIX_TRIE) {
                currentPrefixTrie = prefixTrie;
            } else {
                currentPrefixDocs = prefixPatternDocs;
            }

            // Early return if no data loaded
            if ((currentExactMap == null || currentExactMap.isEmpty())
                && (currentPrefixTrie == null || currentPrefixTrie.isEmpty())
                && (currentPrefixDocs == null || currentPrefixDocs.isEmpty())) {
                return 0;
            }

            int matchCount = 0;

            // First, check exact matches (O(1) lookup)
            if (currentExactMap != null) {
                List<LookupDoc> exactMatches = currentExactMap.get(folderPath);
                if (exactMatches != null) {
                    for (LookupDoc lookupDoc : exactMatches) {
                        docsBuilder.appendInt(lookupDoc.localDocId);
                        if (segmentsBuilder != null) {
                            segmentsBuilder.appendInt(lookupDoc.segmentIndex);
                        }
                        if (positionsBuilder != null) {
                            positionsBuilder.appendInt(position);
                        }
                        matchCount++;
                    }
                }
            }

            // Then, check prefix matches based on approach
            if (approach == ExpressionQueryList.Approach.IN_MEMORY_PREFIX_TRIE) {
                // Use Trie approach (O(m) where m = length of folderPath)
                if (currentPrefixTrie != null) {
                    List<LookupDoc> prefixMatches = currentPrefixTrie.findAllMatchingPrefixes(folderPath);
                    for (LookupDoc lookupDoc : prefixMatches) {
                        docsBuilder.appendInt(lookupDoc.localDocId);
                        if (segmentsBuilder != null) {
                            segmentsBuilder.appendInt(lookupDoc.segmentIndex);
                        }
                        if (positionsBuilder != null) {
                            positionsBuilder.appendInt(position);
                        }
                        matchCount++;
                    }
                }
            } else {
                // Use basic list approach (O(p) where p = number of prefix patterns)
                if (currentPrefixDocs != null) {
                    for (LookupDoc lookupDoc : currentPrefixDocs) {
                        if (matches(folderPath, lookupDoc.prefix)) {
                            docsBuilder.appendInt(lookupDoc.localDocId);
                            if (segmentsBuilder != null) {
                                segmentsBuilder.appendInt(lookupDoc.segmentIndex);
                            }
                            if (positionsBuilder != null) {
                                positionsBuilder.appendInt(position);
                            }
                            matchCount++;
                        }
                    }
                }
            }

            return matchCount;
        } catch (Exception e) {
            Warnings warningsToUse = operatorWarnings != null ? operatorWarnings : warnings;
            warningsToUse.registerException(e);
            return 0;
        }
    }

    @Override
    public int getPositionCount() {
        return block.getPositionCount();
    }

    /**
     * Helper class to store lookup document information
     */
    private static class LookupDoc {
        final int globalDocId;
        final int segmentIndex;
        final int localDocId;
        final String folderPattern;  // Full pattern (e.g., "fola*" or "fan")
        final String prefix;  // Prefix without * (null for exact patterns, "" for "*", "fola" for "fola*")

        LookupDoc(int globalDocId, int segmentIndex, int localDocId, String folderPattern, String prefix) {
            this.globalDocId = globalDocId;
            this.segmentIndex = segmentIndex;
            this.localDocId = localDocId;
            this.folderPattern = folderPattern;
            this.prefix = prefix;
        }
    }

    /**
     * Trie data structure for efficient prefix matching.
     * Stores prefix patterns and allows O(m) lookup where m is the length of the search string.
     */
    private static class PrefixTrie {
        private final TrieNode root = new TrieNode();
        private int size = 0;

        /**
         * Insert a prefix pattern and associated lookup document into the Trie.
         */
        void insert(String prefix, LookupDoc doc) {
            TrieNode current = root;
            for (int i = 0; i < prefix.length(); i++) {
                char c = prefix.charAt(i);
                current = current.children.computeIfAbsent(c, k -> new TrieNode());
            }
            if (current.docs == null) {
                current.docs = new ArrayList<>();
                size++;
            }
            current.docs.add(doc);
        }

        /**
         * Find all prefix patterns that match the given string.
         * A prefix pattern matches if the string starts with the prefix.
         * Special case: empty prefix ("") matches everything.
         * Returns all matching documents.
         */
        List<LookupDoc> findAllMatchingPrefixes(String str) {
            List<LookupDoc> results = new ArrayList<>();

            // Special case: empty prefix ("*" pattern) matches everything
            // Check root node first for empty prefix patterns
            if (root.docs != null) {
                results.addAll(root.docs);
            }

            TrieNode current = root;

            // Traverse the Trie following the string characters
            for (int i = 0; i < str.length(); i++) {
                char c = str.charAt(i);
                TrieNode next = current.children.get(c);
                if (next == null) {
                    // No more matching prefixes
                    break;
                }
                current = next;

                // Collect all documents at this node (prefixes that match up to this point)
                if (current.docs != null) {
                    results.addAll(current.docs);
                }
            }

            return results;
        }

        boolean isEmpty() {
            return size == 0;
        }

        /**
         * Trie node representing a character in the prefix tree.
         */
        private static class TrieNode {
            Map<Character, TrieNode> children = new HashMap<>();
            List<LookupDoc> docs = null;  // Documents matching this prefix (null if not a complete prefix)
        }
    }
}
