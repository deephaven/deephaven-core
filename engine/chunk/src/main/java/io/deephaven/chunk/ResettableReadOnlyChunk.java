package io.deephaven.chunk;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.util.pools.PoolableChunk;

/**
 * {@link Chunk} that may have its backing storage reset to a slice of that belonging to another {@link Chunk} or a
 * native array.
 */
public interface ResettableReadOnlyChunk<ATTR_BASE extends Any> extends ResettableChunk<ATTR_BASE>, PoolableChunk {

    /**
     * Reset the data and bounds of this chunk to a range or sub-range of the specified {@link Chunk}.
     *
     * @param other The other {@link Chunk}
     * @param offset The offset into other
     * @param capacity The capacity this should have after reset
     */
    <ATTR extends ATTR_BASE> Chunk<ATTR> resetFromChunk(Chunk<? extends ATTR> other, int offset, int capacity);

    @Override
    default <ATTR extends ATTR_BASE> Chunk<ATTR> resetFromChunk(WritableChunk<ATTR> other, int offset, int capacity) {
        return resetFromChunk((Chunk<ATTR>) other, offset, capacity);
    }

    @Override
    <ATTR extends ATTR_BASE> Chunk<ATTR> resetFromArray(Object array, int offset, int capacity);

    @Override
    <ATTR extends ATTR_BASE> Chunk<ATTR> resetFromArray(Object array);

    @Override
    <ATTR extends ATTR_BASE> Chunk<ATTR> clear();

    default ResettableByteChunk<ATTR_BASE> asResettableByteChunk() {
        return (ResettableByteChunk<ATTR_BASE>) this;
    }

    default ResettableBooleanChunk<ATTR_BASE> asResettableBooleanChunk() {
        return (ResettableBooleanChunk<ATTR_BASE>) this;
    }

    default ResettableCharChunk<ATTR_BASE> asResettableCharChunk() {
        return (ResettableCharChunk<ATTR_BASE>) this;
    }

    default ResettableShortChunk<ATTR_BASE> asResettableShortChunk() {
        return (ResettableShortChunk<ATTR_BASE>) this;
    }

    default ResettableIntChunk<ATTR_BASE> asResettableIntChunk() {
        return (ResettableIntChunk<ATTR_BASE>) this;
    }

    default ResettableLongChunk<ATTR_BASE> asResettableLongChunk() {
        return (ResettableLongChunk<ATTR_BASE>) this;
    }

    default ResettableFloatChunk<ATTR_BASE> asResettableFloatChunk() {
        return (ResettableFloatChunk<ATTR_BASE>) this;
    }

    default ResettableDoubleChunk<ATTR_BASE> asResettableDoubleChunk() {
        return (ResettableDoubleChunk<ATTR_BASE>) this;
    }

    default <T> ResettableObjectChunk<T, ATTR_BASE> asResettableObjectChunk() {
        return (ResettableObjectChunk<T, ATTR_BASE>) this;
    }
}
