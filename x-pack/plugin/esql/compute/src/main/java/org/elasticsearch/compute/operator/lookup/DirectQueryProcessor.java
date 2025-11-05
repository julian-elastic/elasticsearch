/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.lookup;

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.Warnings;

/**
 * Interface for direct query processing that bypasses Lucene's query framework
 * for direct index access using TermsEnum and PostingsEnum.
 */
public interface DirectQueryProcessor {
    /**
     * Process a single query at the given position using direct Lucene index access.
     * @param position The position in the query list
     * @param indexReader The index reader
     * @param docsBuilder Builder for document IDs
     * @param segmentsBuilder Builder for segment IDs (may be null)
     * @param positionsBuilder Builder for positions
     * @param warnings Warnings collector
     * @return The number of matches found
     */
    int processQuery(
        int position,
        IndexReader indexReader,
        IntVector.Builder docsBuilder,
        IntVector.Builder segmentsBuilder,
        IntVector.Builder positionsBuilder,
        Warnings warnings
    );

    /**
     * Returns the number of positions in this query list
     */
    int getPositionCount();
}
