package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.ResettableWritableChunk;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

/**
 * Writable sources that use simple arrays to back contiguous regions may implement this interface, allowing callers to
 * reach directly into their storage in order to fill them without the need for additional array copies.
 */
public interface ChunkedBackingStoreExposedWritableSource {
    /**
     * Returns true if a given column source exposes a chunked backing store
     * 
     * @param cs the column source to test
     * @return true if the column source exposes a chunked backing store
     */
    static boolean exposesChunkedBackingStore(ColumnSource<?> cs) {
        return cs instanceof ChunkedBackingStoreExposedWritableSource
                && ((ChunkedBackingStoreExposedWritableSource) cs).exposesChunkedBackingStore();
    }

    /**
     * Does this column source provide a chunked exposed backing store?
     * 
     * @return true if this column source provides a chunked backing store, false otherwise
     */
    default boolean exposesChunkedBackingStore() {
        return true;
    }

    /**
     * Resets the given chunk to provide a write-through reference to our backing array.
     * <p>
     * Note: This is unsafe to use if previous tracking has been enabled!
     *
     * @param chunk the writable chunk to reset to our backing array.
     * @param position position that we require
     * @return the first position addressable by the chunk
     */
    long resetWritableChunkToBackingStore(@NotNull ResettableWritableChunk<?> chunk, long position);

    /**
     * Resets the given chunk to provide a write-through reference to our backing array.
     * <p>
     * Note: This is unsafe to use if previous tracking has been enabled!
     *
     * @param chunk the writable chunk to reset to a slice of our backing array.
     * @param position position of the first value in the returned chunk
     * @return the capacity of the returned chunk
     */
    long resetWritableChunkToBackingStoreSlice(@NotNull ResettableWritableChunk<?> chunk, long position);
}
