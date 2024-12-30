//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;

public abstract class ObjectChunkFilter<T> implements ChunkFilter {
    public abstract boolean matches(T value);

    @Override
    public final void filter(
            final Chunk<? extends Values> values,
            final LongChunk<OrderedRowKeys> keys,
            final WritableLongChunk<OrderedRowKeys> results) {
        final ObjectChunk<T, ? extends Values> objectChunk = values.asObjectChunk();
        final int len = objectChunk.size();

        results.setSize(0);
        for (int ii = 0; ii < len; ++ii) {
            if (matches(objectChunk.get(ii))) {
                results.add(keys.get(ii));
            }
        }
    }

    @Override
    public final int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
        final ObjectChunk<T, ? extends Values> objectChunk = values.asObjectChunk();
        final int len = values.size();
        int count = 0;
        for (int ii = 0; ii < len; ++ii) {
            final boolean newResult = matches(objectChunk.get(ii));
            results.set(ii, newResult);
            // count every true value
            count += newResult ? 1 : 0;
        }
        return count;
    }

    @Override
    public final int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
        final ObjectChunk<T, ? extends Values> objectChunk = values.asObjectChunk();
        final int len = values.size();
        int count = 0;
        // Count the values that changed from true to false
        for (int ii = 0; ii < len; ++ii) {
            final boolean result = results.get(ii);
            if (!result) {
                // already false, no need to compute or increment the count
                continue;
            }
            boolean newResult = matches(objectChunk.get(ii));
            results.set(ii, newResult);
            // increment the count if the new result is TRUE
            count += newResult ? 1 : 0;
        }
        return count;
    }
}

