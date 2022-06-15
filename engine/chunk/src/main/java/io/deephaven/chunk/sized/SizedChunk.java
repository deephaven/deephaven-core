/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.chunk.sized;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.util.SafeCloseable;

/**
 * A dynamically typed chunk that can be resized.
 *
 * @param <T> the attribute of the chunk
 */
public class SizedChunk<T extends Any> implements SafeCloseable {
    private final ChunkType chunkType;
    private WritableChunk<T> chunk;

    /**
     * Create a SizedChunk of the given type.
     *
     * @param chunkType the type of the chunk to create
     */
    public SizedChunk(ChunkType chunkType) {
        this.chunkType = chunkType;
    }

    /**
     * Get the underlying chunk.
     *
     * @return the underlying chunk.
     */
    public WritableChunk<T> get() {
        return chunk;
    }

    /**
     * Ensure the underlying chunk has a capacity of at least {@code capacity}.
     *
     * The data and size of the returned chunk are undefined.
     *
     * @param capacity the minimum capacity for the chunk.
     *
     * @return the underlying chunk
     */
    public WritableChunk<T> ensureCapacity(int capacity) {
        if (chunk == null || capacity > chunk.capacity()) {
            if (chunk != null) {
                chunk.close();
            }
            chunk = chunkType.makeWritableChunk(capacity);
        }
        return chunk;
    }

    /**
     * Ensure the underlying chunk has a capacity of at least {@code capacity}.
     *
     * If the chunk has existing data, then it is copied to the new chunk.
     *
     * If the underlying chunk already exists, then the size of the chunk is the original size. If the chunk did not
     * exist, then the size of the returned chunk is zero.
     *
     * @param capacity the minimum capacity for the chunk.
     *
     * @return the underlying chunk
     */
    public WritableChunk<T> ensureCapacityPreserve(int capacity) {
        if (chunk == null || capacity > chunk.capacity()) {
            final WritableChunk<T> oldChunk = chunk;
            chunk = chunkType.makeWritableChunk(capacity);
            if (oldChunk != null) {
                chunk.copyFromChunk(oldChunk, 0, 0, oldChunk.size());
                chunk.setSize(oldChunk.size());
                oldChunk.close();
            } else {
                chunk.setSize(0);
            }
        }
        return chunk;
    }

    @Override
    public void close() {
        if (chunk != null) {
            chunk.close();
            chunk = null;
        }
    }
}
