package io.deephaven.chunk.util;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.util.datastructures.LongRangeAbortableConsumer;
import io.deephaven.util.datastructures.LongRangeIterator;

public class LongChunkRangeIterator implements LongRangeIterator {

    private final LongChunk<? extends Any> ck;
    private final int lastRangeStart;

    private int previousRangeStart;

    public LongChunkRangeIterator(final LongChunk<? extends Any> ck) {
        this.ck = ck;
        lastRangeStart = ck.size() - 2;
        previousRangeStart = -2;
    }

    @Override
    public boolean hasNext() {
        return previousRangeStart < lastRangeStart;
    }

    @Override
    public void next() {
        previousRangeStart += 2;
    }

    @Override
    public long start() {
        return ck.get(previousRangeStart);
    }

    @Override
    public long end() {
        return ck.get(previousRangeStart + 1);
    }

    @Override
    public boolean forEachLongRange(final LongRangeAbortableConsumer lrc) {
        while (previousRangeStart < lastRangeStart) {
            previousRangeStart += 2;
            final long s = ck.get(previousRangeStart);
            final long e = ck.get(previousRangeStart + 1);
            if (!lrc.accept(s, e)) {
                return false;
            }
        }
        return true;
    }
}
