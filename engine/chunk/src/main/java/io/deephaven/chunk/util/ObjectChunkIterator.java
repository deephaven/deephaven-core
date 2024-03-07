//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.chunk.util;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Any;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;

public class ObjectChunkIterator<TYPE> implements Iterator<TYPE> {

    private final ObjectChunk<TYPE, ? extends Any> chunk;
    private final int limit;

    private int next;

    public ObjectChunkIterator(@NotNull final ObjectChunk<TYPE, ? extends Any> chunk) {
        this(chunk, 0, chunk.size());
    }

    public ObjectChunkIterator(@NotNull final ObjectChunk<TYPE, ? extends Any> chunk, final int offset,
            final int length) {
        this.chunk = chunk;
        this.next = offset;
        this.limit = offset + length;
    }

    @Override
    public boolean hasNext() {
        return next < limit;
    }

    @Override
    public TYPE next() {
        return chunk.get(next++);
    }
}
