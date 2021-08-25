package io.deephaven.db.v2.sources.chunk;

import io.deephaven.db.v2.sources.chunk.Attributes.Any;

/**
 * {@link ChunkChunk} that may have its backing storage reset to a slice of that belonging to another {@link ChunkChunk}
 * or a native array.
 */
public interface ResettableChunkChunk<ATTR extends Any> extends ChunkChunk<ATTR> {

    /**
     * Reset the data and bounds of this chunk to a range or sub-range of the specified {@link ChunkChunk}.
     *
     * @param other The other {@link ChunkChunk}
     * @param offset The offset into other
     * @param capacity The capacity this should have after reset
     */
    void resetFromChunk(ChunkChunk<ATTR> other, int offset, int capacity);

    /**
     * Reset the data and bounds of this chunk to a range or sub-range of the specified array.
     *
     * @param array The array
     * @param offset The offset into array
     * @param capacity The capacity this should have after reset
     */
    void resetFromArray(Object array, int offset, int capacity);

    default ResettableByteChunkChunk<ATTR> asResettableByteChunkChunk() {
        return (ResettableByteChunkChunk<ATTR>) this;
    }

    default ResettableBooleanChunkChunk<ATTR> asResettableBooleanChunkChunk() {
        return (ResettableBooleanChunkChunk<ATTR>) this;
    }

    default ResettableCharChunkChunk<ATTR> asResettableCharChunkChunk() {
        return (ResettableCharChunkChunk<ATTR>) this;
    }

    default ResettableShortChunkChunk<ATTR> asResettableShortChunkChunk() {
        return (ResettableShortChunkChunk<ATTR>) this;
    }

    default ResettableIntChunkChunk<ATTR> asResettableIntChunkChunk() {
        return (ResettableIntChunkChunk<ATTR>) this;
    }

    default ResettableLongChunkChunk<ATTR> asResettableLongChunkChunk() {
        return (ResettableLongChunkChunk<ATTR>) this;
    }

    default ResettableFloatChunkChunk<ATTR> asResettableFloatChunkChunk() {
        return (ResettableFloatChunkChunk<ATTR>) this;
    }

    default ResettableDoubleChunkChunk<ATTR> asResettableDoubleChunkChunk() {
        return (ResettableDoubleChunkChunk<ATTR>) this;
    }

    default <T> ResettableObjectChunkChunk<T, ATTR> asResettableObjectChunkChunk() {
        return (ResettableObjectChunkChunk<T, ATTR>) this;
    }
}
