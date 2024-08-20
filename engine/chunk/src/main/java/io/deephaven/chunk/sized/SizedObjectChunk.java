//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit SizedCharChunk and run "./gradlew replicateSourcesAndChunks" to regenerate
//
// @formatter:off
package io.deephaven.chunk.sized;

import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.Nullable;

/**
 * A wrapper for a Object chunk that allows you to resize the chunk to a capacity.
 *
 * @param <ATTR> the chunk's attribute
 */
public final class SizedObjectChunk<T, ATTR extends Any> implements SafeCloseable {
    private WritableObjectChunk<T, ATTR> chunk;

    public SizedObjectChunk() {}

    public SizedObjectChunk(final int initialSize) {
        chunk = WritableObjectChunk.makeWritableChunk(initialSize);
    }

    /**
     * Get the underlying chunk.
     *
     * @return the underlying chunk. May be {@code null} if the chunk has not been initialized.
     */
    @Nullable
    public WritableObjectChunk<T, ATTR> get() {
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
    public WritableObjectChunk<T, ATTR> ensureCapacity(int capacity) {
        if (chunk == null || capacity > chunk.capacity()) {
            if (chunk != null) {
                chunk.close();
            }
            chunk = WritableObjectChunk.makeWritableChunk(capacity);
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
    public WritableObjectChunk<T, ATTR> ensureCapacityPreserve(int capacity) {
        if (chunk == null || capacity > chunk.capacity()) {
            final WritableObjectChunk<T, ATTR> oldChunk = chunk;
            chunk = WritableObjectChunk.makeWritableChunk(capacity);
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
