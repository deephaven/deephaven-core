package io.deephaven.engine.structures.chunk.util;

import io.deephaven.engine.structures.chunk.IntChunk;

import java.util.PrimitiveIterator;

public class IntChunkLongIterator implements PrimitiveIterator.OfLong {
    private final IntChunk ck;
    private int i;
    public IntChunkLongIterator(final IntChunk ck) {
        this.ck = ck;
        i = 0;
    }
    @Override
    public boolean hasNext() {
        return i < ck.size();
    }
    @Override
    public long nextLong() {
        return ck.get(i++);
    }
}
