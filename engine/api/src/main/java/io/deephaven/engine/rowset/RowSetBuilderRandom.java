/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.rowset;

import io.deephaven.util.datastructures.LongRangeIterator;
import io.deephaven.engine.chunk.Attributes;
import io.deephaven.engine.chunk.IntChunk;
import io.deephaven.engine.chunk.LongChunk;
import io.deephaven.engine.chunk.util.IntChunkLongIterator;
import io.deephaven.engine.chunk.util.LongChunkIterator;

import java.util.PrimitiveIterator;

/**
 * Builder interface for {@link RowSet} construction in arbitrary order.
 */
public interface RowSetBuilderRandom {

    WritableRowSet build();

    void addKey(long rowKey);

    void addRange(long firstRowKey, long lastRowKey);

    default void addKeys(final PrimitiveIterator.OfLong it) {
        while (it.hasNext()) {
            final long v = it.nextLong();
            addKey(v);
        }
    }

    default void addRanges(final LongRangeIterator it) {
        while (it.hasNext()) {
            it.next();
            addRange(it.start(), it.end());
        }
    }

    default void addRowKeysChunk(final LongChunk<? extends Attributes.RowKeys> chunk) {
        addKeys(new LongChunkIterator(chunk));
    }

    default void addRowKeysChunk(final IntChunk<? extends Attributes.RowKeys> chunk) {
        addKeys(new IntChunkLongIterator(chunk));
    }

    default void addOrderedRowKeysChunk(final LongChunk<? extends Attributes.OrderedRowKeys> chunk) {
        addRowKeysChunk(chunk);
    }

    default void addOrderedRowKeysChunk(final IntChunk<? extends Attributes.OrderedRowKeys> chunk) {
        addRowKeysChunk(chunk);
    }

    default void addRowSet(final RowSet rowSet) {
        Helper.add(this, rowSet);
    }

    class Helper {
        private static void add(final RowSetBuilderRandom builder, final RowSet rowSet) {
            final RowSet.RangeIterator it = rowSet.rangeIterator();
            while (it.hasNext()) {
                final long start = it.next();
                final long end = it.currentRangeEnd();
                builder.addRange(start, end);
            }
        }
    }
}
