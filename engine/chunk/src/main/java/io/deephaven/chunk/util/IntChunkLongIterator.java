package io.deephaven.chunk.util;

import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.attributes.Any;

import java.util.PrimitiveIterator;

public class IntChunkLongIterator implements PrimitiveIterator.OfLong {

    private final IntChunk<? extends Any> ck;
    private int pos;

    public IntChunkLongIterator(final IntChunk<? extends Any> ck) {
        this.ck = ck;
        pos = 0;
    }

    @Override
    public boolean hasNext() {
        return pos < ck.size();
    }

    @Override
    public long nextLong() {
        return ck.get(pos++);
    }
}
