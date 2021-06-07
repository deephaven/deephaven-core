/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit SizedCharChunk and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.sources.chunk.sized;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.WritableBooleanChunk;
import io.deephaven.util.SafeCloseable;

/**
 * A wrapper for a boolean chunk that allows you to resize the chunk to a capacity.
 *
 * @param <T> the chunk's attribute
 */
public final class SizedBooleanChunk<T extends Attributes.Any> implements SafeCloseable {
    private WritableBooleanChunk<T> chunk;

    /**
     * Get the underlying chunk.
     *
     * @return the underlying chunk.
     */
    public WritableBooleanChunk<T> get() {
        return chunk;
    }

    /**
     * Ensure the underlying chunk has a capacity of at least {@code capacity}, preserving data.
     *
     * The data and size of the returned chunk are undefined.  If you must maintain the data, then use
     * {@link #ensureCapacityPreserve(int)}.
     *
     * @param capacity the minimum capacity for the chunk.
     *
     * @return the underlying chunk
     */
    public WritableBooleanChunk<T> ensureCapacity(int capacity) {
        if (chunk == null || capacity > chunk.capacity()) {
            if (chunk != null) {
                chunk.close();
            }
            chunk = WritableBooleanChunk.makeWritableChunk(capacity);
        }
        return chunk;
    }

    /**
     * Ensure the underlying chunk has a capacity of at least {@code capacity}.
     *
     * If the chunk has existing data, then it is copied to the new chunk.
     *
     * If the underlying chunk already exists, then the size of the chunk is the original size.  If the chunk did not
     * exist, then the size of the returned chunk is zero.
     *
     * @param capacity the minimum capacity for the chunk.
     *
     * @return the underlying chunk
     */
    public WritableBooleanChunk<T> ensureCapacityPreserve(int capacity) {
        if (chunk == null || capacity > chunk.capacity()) {
            final WritableBooleanChunk<T> oldChunk = chunk;
            chunk = WritableBooleanChunk.makeWritableChunk(capacity);
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
