package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.ResettableWritableChunk;
import org.jetbrains.annotations.NotNull;

public interface ChunkedBackingStoreExposedWritableSource {
    /**
     * Resets the given chunk to provide a write-through reference to our backing array.
     * <p>
     * Note: This is unsafe to use if previous tracking has been enabled!
     *
     * @param chunk    the writable chunk to reset to our backing array.
     * @param position position that we require
     * @return the first position addressable by the chunk
     */
    long resetWritableChunkToBackingStore(@NotNull ResettableWritableChunk<?> chunk, long position);
}
