//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkFilter and run "./gradlew replicateChunkFilters" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;

public abstract class DoubleChunkFilter implements ChunkFilter {
    public abstract boolean matches(double value);

    @Override
    public final void filter(
            final Chunk<? extends Values> values,
            final LongChunk<OrderedRowKeys> keys,
            final WritableLongChunk<OrderedRowKeys> results) {
        final DoubleChunk<? extends Values> doubleChunk = values.asDoubleChunk();
        final int len = doubleChunk.size();

        results.setSize(0);
        for (int ii = 0; ii < len; ++ii) {
            if (matches(doubleChunk.get(ii))) {
                results.add(keys.get(ii));
            }
        }
    }

    @Override
    public final int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
        final DoubleChunk<? extends Values> doubleChunk = values.asDoubleChunk();
        final int len = values.size();
        int count = 0;
        for (int ii = 0; ii < len; ++ii) {
            final boolean newResult = matches(doubleChunk.get(ii));
            results.set(ii, newResult);
            // count every true value
            count += newResult ? 1 : 0;
        }
        return count;
    }

    @Override
    public final int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
        final DoubleChunk<? extends Values> doubleChunk = values.asDoubleChunk();
        final int len = values.size();
        int count = 0;
        // Count the values that changed from true to false
        for (int ii = 0; ii < len; ++ii) {
            final boolean result = results.get(ii);
            if (!result) {
                // already false, no need to compute or increment the count
                continue;
            }
            boolean newResult = matches(doubleChunk.get(ii));
            results.set(ii, newResult);
            // increment the count if the new result is TRUE
            count += newResult ? 1 : 0;
        }
        return count;
    }
}

