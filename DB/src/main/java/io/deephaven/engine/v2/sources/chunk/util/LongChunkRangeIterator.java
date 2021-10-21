package io.deephaven.engine.v2.sources.chunk.util;

import io.deephaven.engine.v2.sources.chunk.Attributes;
import io.deephaven.engine.v2.sources.chunk.Attributes.OrderedRowKeyRanges;
import io.deephaven.engine.v2.sources.chunk.LongChunk;
import io.deephaven.engine.v2.utils.LongRangeAbortableConsumer;

public class LongChunkRangeIterator implements io.deephaven.engine.v2.utils.LongRangeIterator {
    private final LongChunk<Attributes.OrderedRowKeyRanges> ck;
    private final int lastj;
    private int j;
    public LongChunkRangeIterator(final LongChunk<OrderedRowKeyRanges> ck) {
        this.ck = ck;
        lastj = ck.size() - 2;
        j = -2;
    }
    @Override public boolean hasNext() {
        return j < lastj;
    }
    @Override public void next() {
        j += 2;
    }
    @Override public long start() {
        return ck.get(j);
    }
    @Override public long end() {
        return ck.get(j + 1);
    }
    @Override public boolean forEachLongRange(final LongRangeAbortableConsumer lrc) {
        while (j < lastj) {
            j += 2;
            final long s = ck.get(j);
            final long e = ck.get(j+1);
            if (!lrc.accept(s, e)) {
                return false;
            }
        }
        return true;
    }
}
