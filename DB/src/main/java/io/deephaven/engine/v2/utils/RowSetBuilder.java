/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.utils;

import gnu.trove.procedure.TLongProcedure;
import io.deephaven.engine.structures.RowSequence;
import io.deephaven.engine.v2.sources.chunk.Attributes;
import io.deephaven.engine.v2.sources.chunk.IntChunk;
import io.deephaven.engine.v2.sources.chunk.LongChunk;
import io.deephaven.engine.v2.sources.chunk.util.IntChunkLongIterator;
import io.deephaven.engine.v2.sources.chunk.util.LongChunkIterator;

import java.util.PrimitiveIterator;

/**
 * Builder interface for {@link RowSet} construction in arbitrary order.
 */
public interface RowSetBuilder {

    MutableRowSet build();

    void addKey(long key);

    void addRange(long firstKey, long lastKey);

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

    default void addKeyIndicesChunk(final LongChunk<Attributes.RowKeys> chunk) {
        addKeys(new LongChunkIterator(chunk));
    }

    default void addKeyIndicesChunk(final IntChunk<Attributes.RowKeys> chunk) {
        addKeys(new IntChunkLongIterator(chunk));
    }

    default void addOrderedKeyIndicesChunk(final LongChunk<Attributes.OrderedRowKeys> chunk) {
        addKeys(new LongChunkIterator(chunk));
    }

    default void addRowSet(final RowSet rowSet) {
        Helper.add(this, rowSet);
    }

    /**
     * {@link RowSetBuilder} suitable for inserting ranges in no particular order.
     */
    interface RandomBuilder extends RowSetBuilder {
    }

    class Helper {
        public static void add(final RowSetBuilder builder, final RowSet rowSet) {
            final TrackingMutableRowSet.RangeIterator it = rowSet.rangeIterator();
            while (it.hasNext()) {
                final long start = it.next();
                final long end = it.currentRangeEnd();
                builder.addRange(start, end);
            }
        }
    }
}
