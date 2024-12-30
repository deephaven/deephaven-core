//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;

public abstract class CharChunkFilter implements ChunkFilter {
    public abstract boolean matches(char value);

    @Override
    public final void filter(
            final Chunk<? extends Values> values,
            final LongChunk<OrderedRowKeys> keys,
            final WritableLongChunk<OrderedRowKeys> results) {
        final CharChunk<? extends Values> charChunk = values.asCharChunk();
        final int len = charChunk.size();

        results.setSize(0);
        for (int ii = 0; ii < len; ++ii) {
            if (matches(charChunk.get(ii))) {
                results.add(keys.get(ii));
            }
        }
    }

    @Override
    public final int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
        final CharChunk<? extends Values> charChunk = values.asCharChunk();
        final int len = values.size();
        int count = 0;
        for (int ii = 0; ii < len; ++ii) {
            final boolean newResult = matches(charChunk.get(ii));
            results.set(ii, newResult);
            // count every true value
            count += newResult ? 1 : 0;
        }
        return count;
    }

    @Override
    public final int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
        final CharChunk<? extends Values> charChunk = values.asCharChunk();
        final int len = values.size();
        int count = 0;
        // Count the values that changed from true to false
        for (int ii = 0; ii < len; ++ii) {
            final boolean result = results.get(ii);
            if (!result) {
                // already false, no need to compute or increment the count
                continue;
            }
            boolean newResult = matches(charChunk.get(ii));
            results.set(ii, newResult);
            // increment the count if the new result is TRUE
            count += newResult ? 1 : 0;
        }
        return count;
    }
}

