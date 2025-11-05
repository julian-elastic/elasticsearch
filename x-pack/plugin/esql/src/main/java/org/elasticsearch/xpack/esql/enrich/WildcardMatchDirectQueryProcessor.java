/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.compute.operator.lookup.DirectQueryProcessor;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.internal.AliasFilter;

public class WildcardMatchDirectQueryProcessor implements DirectQueryProcessor {
    private final MappedFieldType termField;
    private final MappedFieldType wildcardField;
    private final SearchExecutionContext context;
    private final BytesRefBlock block;
    private final ClusterService clusterService;
    private final AliasFilter aliasFilter;
    private final Warnings warnings;
    private final WildcardMatchQuery wildcardMatchQuery;

    public WildcardMatchDirectQueryProcessor(
        MappedFieldType termField,
        MappedFieldType wildcardField,
        SearchExecutionContext context,
        Block block,
        ClusterService clusterService,
        AliasFilter aliasFilter,
        Warnings warnings
    ) {
        this.termField = termField;
        this.wildcardField = wildcardField;
        this.context = context;
        this.block = (BytesRefBlock) block;
        this.clusterService = clusterService;
        this.aliasFilter = aliasFilter;
        this.warnings = warnings;
        // Create a WildcardMatchQuery instance to reuse its processQuery method
        // We'll pass the searchValue dynamically in processQuery
        this.wildcardMatchQuery = new WildcardMatchQuery("", termField, wildcardField, context);
    }

    /**
     * Process a single query at the given position using direct Lucene index access.
     * This method bypasses Lucene's query framework entirely and directly accesses
     * the inverted index using TermsEnum and PostingsEnum for maximum performance.
     */
    public int processQuery(
        int position,
        IndexReader indexReader,
        IntVector.Builder docsBuilder,
        IntVector.Builder segmentsBuilder,
        IntVector.Builder positionsBuilder,
        Warnings operatorWarnings
    ) {
        try {
            final int valueCount = block.getValueCount(position);
            if (valueCount != 1) {
                return 0; // Skip multi-value positions and null positions
            }
            final int firstValueIndex = block.getFirstValueIndex(position);
            BytesRef termBytes = block.getBytesRef(firstValueIndex, new BytesRef());
            String searchValue = termBytes.utf8ToString();

            // Use operator warnings if provided, otherwise use constructor warnings
            Warnings warningsToUse = operatorWarnings != null ? operatorWarnings : warnings;

            return wildcardMatchQuery.processQuery(
                searchValue,
                indexReader,
                docsBuilder,
                segmentsBuilder,
                positionsBuilder,
                position,
                warningsToUse
            );
        } catch (Exception e) {
            Warnings warningsToUse = operatorWarnings != null ? operatorWarnings : warnings;
            warningsToUse.registerException(e);
            return 0;
        }
    }

    public int getPositionCount() {
        return block.getPositionCount();
    }
}
