package io.deephaven.chunk.util;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Any;

import java.util.PrimitiveIterator;

public class LongChunkIterator implements PrimitiveIterator.OfLong {

    private final LongChunk<? extends Any> ck;
    private final int end;

    private int start;

    public LongChunkIterator(final LongChunk<? extends Any> ck) {
        this(ck, 0, ck.size());
    }

    public LongChunkIterator(final LongChunk<? extends Any> ck, int start, int size) {
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
