/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.IntChunk;
import io.deephaven.db.v2.sources.chunk.LongChunk;
import io.deephaven.db.v2.sources.chunk.util.IntChunkLongIterator;
import io.deephaven.db.v2.sources.chunk.util.LongChunkIterator;

import java.util.PrimitiveIterator;

public interface IndexBuilder {
    Index getIndex();

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

    default void addKeyIndicesChunk(final LongChunk<Attributes.KeyIndices> chunk) {
        addKeys(new LongChunkIterator(chunk));
    }

    default void addKeyIndicesChunk(final IntChunk<Attributes.KeyIndices> chunk) {
        addKeys(new IntChunkLongIterator(chunk));
    }

    default void addOrderedKeyIndicesChunk(final LongChunk<Attributes.OrderedKeyIndices> chunk) {
        addKeys(new LongChunkIterator(chunk));
    }

    default void addIndex(final Index idx) {
        Helper.add(this, idx);
    }

    class Helper {
        public static void add(final IndexBuilder builder, final Index idx) {
            final Index.RangeIterator it = idx.rangeIterator();
            while (it.hasNext()) {
                final long start = it.next();
                final long end = it.currentRangeEnd();
                builder.addRange(start, end);
            }
        }
    }
}
