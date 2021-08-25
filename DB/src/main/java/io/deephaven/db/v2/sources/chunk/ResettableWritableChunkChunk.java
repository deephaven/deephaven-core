package io.deephaven.db.v2.sources.chunk;

import io.deephaven.db.v2.sources.chunk.Attributes.Any;

/**
 * {@link WritableChunkChunk} that may have its backing storage reset to a slice of that belonging
 * to another {@link WritableChunkChunk} or a native array.
 */
public interface ResettableWritableChunkChunk<ATTR extends Any> extends WritableChunkChunk<ATTR> {

    /**
     * Reset the data and bounds of this chunk to a range or sub-range of the specified
     * {@link WritableChunkChunk}.
     *
     * @param other The other {@link WritableChunkChunk}
     * @param offset The offset into other
     * @param capacity The capacity this should have after reset
     */
    void resetFromChunk(WritableChunkChunk<ATTR> other, int offset, int capacity);

    /**
     * Reset the data and bounds of this chunk to a range or sub-range of the specified array.
     *
     * @param array The array
     * @param offset The offset into array
     * @param capacity The capacity this should have after reset
     */
    void resetFromArray(Object array, int offset, int capacity);

    default ResettableWritableByteChunkChunk<ATTR> asResettableWritableByteChunkChunk() {
        return (ResettableWritableByteChunkChunk<ATTR>) this;
    }

    default ResettableWritableBooleanChunkChunk<ATTR> asResettableWritableBooleanChunkChunk() {
        return (ResettableWritableBooleanChunkChunk<ATTR>) this;
    }

    default ResettableWritableCharChunkChunk<ATTR> asResettableWritableCharChunkChunk() {
        return (ResettableWritableCharChunkChunk<ATTR>) this;
    }

    default ResettableWritableShortChunkChunk<ATTR> asResettableWritableShortChunkChunk() {
        return (ResettableWritableShortChunkChunk<ATTR>) this;
    }

    default ResettableWritableIntChunkChunk<ATTR> asResettableWritableIntChunkChunk() {
        return (ResettableWritableIntChunkChunk<ATTR>) this;
    }

    default ResettableWritableLongChunkChunk<ATTR> asResettableWritableLongChunkChunk() {
        return (ResettableWritableLongChunkChunk<ATTR>) this;
    }

    default ResettableWritableFloatChunkChunk<ATTR> asResettableWritableFloatChunkChunk() {
        return (ResettableWritableFloatChunkChunk<ATTR>) this;
    }

    default ResettableWritableDoubleChunkChunk<ATTR> asResettableWritableDoubleChunkChunk() {
        return (ResettableWritableDoubleChunkChunk<ATTR>) this;
    }

    default <T> ResettableWritableObjectChunkChunk<T, ATTR> asResettableWritableObjectChunkChunk() {
        return (ResettableWritableObjectChunkChunk<T, ATTR>) this;
    }
}
