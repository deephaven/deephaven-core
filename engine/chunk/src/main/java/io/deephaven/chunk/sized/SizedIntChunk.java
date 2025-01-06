//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit SizedCharChunk and run "./gradlew replicateSourcesAndChunks" to regenerate
//
// @formatter:off
package io.deephaven.chunk.sized;

import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.Nullable;

/**
 * A wrapper for a int chunk that allows you to resize the chunk to a capacity.
 *
 * @param <T> the chunk's attribute
 */
public final class SizedIntChunk<T extends Any> implements SafeCloseable {
    private WritableIntChunk<T> chunk;

    public SizedIntChunk() {}

    public SizedIntChunk(final int initialSize) {
        chunk = WritableIntChunk.makeWritableChunk(initialSize);
    }

    /**
     * Get the underlying chunk.
     *
     * @return the underlying chunk. May be {@code null} if the chunk has not been initialized.
     */
    @Nullable
    public WritableIntChunk<T> get() {
        return chunk;
    }

    /**
     * Ensure the underlying chunk has a capacity of at least {@code capacity}, preserving data.
     *
     * The data and size of the returned chunk are undefined. If you must maintain the data, then use
     * {@link #ensureCapacityPreserve(int)}.
     *
     * @param capacity the minimum capacity for the chunk.
     *
     * @return the underlying chunk
     */
    public WritableIntChunk<T> ensureCapacity(int capacity) {
        if (chunk == null || capacity > chunk.capacity()) {
            if (chunk != null) {
                chunk.close();
            }
            chunk = WritableIntChunk.makeWritableChunk(capacity);
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
    public WritableIntChunk<T> ensureCapacityPreserve(int capacity) {
        if (chunk == null || capacity > chunk.capacity()) {
            final WritableIntChunk<T> oldChunk = chunk;
            chunk = WritableIntChunk.makeWritableChunk(capacity);
            if (oldChunk != null) {
                chunk.copyFromTypedChunk(oldChunk, 0, 0, oldChunk.size());
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
