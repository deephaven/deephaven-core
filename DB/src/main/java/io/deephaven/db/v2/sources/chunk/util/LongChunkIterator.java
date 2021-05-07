package io.deephaven.db.v2.sources.chunk.util;

import io.deephaven.db.v2.sources.chunk.LongChunk;

import java.util.PrimitiveIterator;

public class LongChunkIterator implements PrimitiveIterator.OfLong {
    private final LongChunk ck;
    private int start;
    private final int end;

    public LongChunkIterator(final LongChunk ck) {
        this(ck, 0, ck.size());
    }

    public LongChunkIterator(final LongChunk ck, int start, int size) {
        this.ck = ck;
        this.start = start;
        this.end = start + size;
    }

    @Override
    public boolean hasNext() {
        return start < end;
    }
    @Override
    public long nextLong() {
        return ck.get(start++);
    }
}
