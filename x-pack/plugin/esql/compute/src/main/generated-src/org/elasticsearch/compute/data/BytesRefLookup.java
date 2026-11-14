/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

// begin generated imports
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
// end generated imports

/**
 * Generic {@link Block#lookup} implementation {@link BytesRefBlock}s.
 * This class is generated. Edit {@code X-Lookup.java.st} instead.
 */
public final class BytesRefLookup implements ReleasableIterator<BytesRefBlock> {
    // BytesRefArray reserves 3 value bytes per entry up front and adds 4-byte int offsets once value lengths differ.
    private static final int PREALLOCATED_BYTES_PER_VALUE = 3 + Integer.BYTES;
    private final BytesRef firstScratch = new BytesRef();
    private final BytesRef valueScratch = new BytesRef();
    // BytesRefArray allocates in whole pages, so its estimatedBytes can pass the target long before the values do.
    // Blocks are sized by the bytes copied instead: each entry's value bytes plus its int offset.
    private long copiedBytes;
    private final BytesRefBlock values;
    private final IntBlock positions;
    private final long targetByteSize;
    private final int initialPositions;
    private int position;

    private BytesRef first;
    private int valuesInPosition;

    public BytesRefLookup(BytesRefBlock values, IntBlock positions, ByteSizeValue targetBlockSize) {
        values.incRef();
        positions.incRef();
        this.values = values;
        this.positions = positions;
        this.targetByteSize = targetBlockSize.getBytes();
        // Preallocated capacity counts toward the builder's estimatedBytes, so preallocate only as many positions as fit under the target.
        long fittingPositions = (targetByteSize - RamUsageEstimator.NUM_BYTES_ARRAY_HEADER - 1) / PREALLOCATED_BYTES_PER_VALUE;
        this.initialPositions = (int) Math.min(Integer.MAX_VALUE, Math.max(1, fittingPositions));
    }

    @Override
    public boolean hasNext() {
        return position < positions.getPositionCount();
    }

    @Override
    public BytesRefBlock next() {
        int initialSize = Math.min(positions.getPositionCount() - position, initialPositions);
        copiedBytes = 0;
        try (BytesRefBlock.Builder builder = positions.blockFactory().newBytesRefBlockBuilder(initialSize)) {
            int count = 0;
            while (position < positions.getPositionCount()) {
                int start = positions.getFirstValueIndex(position);
                int end = start + positions.getValueCount(position);
                valuesInPosition = 0;
                for (int i = start; i < end; i++) {
                    copy(builder, positions.getInt(i));
                }
                switch (valuesInPosition) {
                    case 0 -> builder.appendNull();
                    case 1 -> builder.appendBytesRef(first);
                    default -> builder.endPositionEntry();
                }
                position++;
                // Every entry takes an int offset, including the empty value a null writes.
                copiedBytes += (long) Integer.BYTES * Math.max(1, valuesInPosition);
                // TOOD what if the estimate is super huge? should we break even with less than MIN_TARGET?
                if (++count > Operator.MIN_TARGET_PAGE_SIZE && copiedBytes >= targetByteSize) {
                    break;
                }
            }
            return builder.build();
        }
    }

    private void copy(BytesRefBlock.Builder builder, int valuePosition) {
        if (valuePosition >= values.getPositionCount()) {
            return;
        }
        int start = values.getFirstValueIndex(valuePosition);
        int end = start + values.getValueCount(valuePosition);
        for (int i = start; i < end; i++) {
            if (valuesInPosition == 0) {
                first = values.getBytesRef(i, firstScratch);
                copiedBytes += first.length;
                valuesInPosition++;
                continue;
            }
            if (valuesInPosition == 1) {
                builder.beginPositionEntry();
                builder.appendBytesRef(first);
            }
            if (valuesInPosition > Block.MAX_LOOKUP) {
                // TODO replace this with a warning and break
                throw new IllegalArgumentException("Found a single entry with " + valuesInPosition + " entries");
            }
            BytesRef value = values.getBytesRef(i, valueScratch);
            copiedBytes += value.length;
            builder.appendBytesRef(value);
            valuesInPosition++;
        }
    }

    @Override
    public void close() {
        Releasables.close(values, positions);
    }
}
