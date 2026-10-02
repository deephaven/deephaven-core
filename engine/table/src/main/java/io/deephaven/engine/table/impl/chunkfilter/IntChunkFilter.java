//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkFilter and run "./gradlew replicateChunkFilters" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;

/**
 * A {@link ChunkFilter} for int values that tests each value with {@link #matches(int)}.
 * <p>
 * The loops in the {@code filterLoops} region are shared by every subclass, so once a few filter types have run through
 * them, the JIT sees many receivers at the {@code matches} call and leaves it as a virtual call per value.
 * {@code ReplicateChunkFilters} copies this region into the filters whose {@code matches} is only a compare or two (the
 * range comparators and the one-to-three value match filters), so that each copy calls {@code matches} on one class
 * only. Edit the loops here and run {@code ./gradlew replicateChunkFilters} to update the copies.
 */
public abstract class IntChunkFilter implements ChunkFilter {
    public abstract boolean matches(int value);

    // region filterLoops
    @Override
    public void filter(
            final Chunk<? extends Values> values,
            final LongChunk<OrderedRowKeys> keys,
            final WritableLongChunk<OrderedRowKeys> results) {
        final IntChunk<? extends Values> intChunk = values.asIntChunk();
        final int len = intChunk.size();

        results.setSize(0);
        for (int ii = 0; ii < len; ++ii) {
            if (matches(intChunk.get(ii))) {
                results.add(keys.get(ii));
            }
        }
    }

    @Override
    public int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
        final IntChunk<? extends Values> intChunk = values.asIntChunk();
        final int len = values.size();
        int count = 0;
        for (int ii = 0; ii < len; ++ii) {
            final boolean newResult = matches(intChunk.get(ii));
            results.set(ii, newResult);
            // count every true value
            count += newResult ? 1 : 0;
        }
        return count;
    }

    @Override
    public int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
        final IntChunk<? extends Values> intChunk = values.asIntChunk();
        final int len = values.size();
        int count = 0;
        // Count the values that remain true
        for (int ii = 0; ii < len; ++ii) {
            final boolean result = results.get(ii);
            if (!result) {
                // already false, no need to compute or increment the count
                continue;
            }
            boolean newResult = matches(intChunk.get(ii));
            results.set(ii, newResult);
            // increment the count if the new result is TRUE
            count += newResult ? 1 : 0;
        }
        return count;
    }
    // endregion filterLoops
}
