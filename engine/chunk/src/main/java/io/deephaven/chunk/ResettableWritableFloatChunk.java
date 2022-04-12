/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ResettableWritableCharChunk and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.util.pools.MultiChunkPool;
import io.deephaven.util.type.ArrayTypeUtils;

/**
 * {@link ResettableWritableChunk} implementation for float data.
 */
public final class ResettableWritableFloatChunk<ATTR_BASE extends Any> extends WritableFloatChunk implements ResettableWritableChunk<ATTR_BASE> {

    public static <ATTR_BASE extends Any> ResettableWritableFloatChunk<ATTR_BASE> makeResettableChunk() {
        return MultiChunkPool.forThisThread().getFloatChunkPool().takeResettableWritableFloatChunk();
    }

    public static <ATTR_BASE extends Any> ResettableWritableFloatChunk<ATTR_BASE> makeResettableChunkForPool() {
        return new ResettableWritableFloatChunk<>();
    }

    private ResettableWritableFloatChunk(float[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    private ResettableWritableFloatChunk() {
        this(ArrayTypeUtils.EMPTY_FLOAT_ARRAY, 0, 0);
    }

    @Override
    public final ResettableWritableFloatChunk slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new ResettableWritableFloatChunk<>(data, this.offset + offset, capacity);
    }

    @Override
    public final <ATTR extends ATTR_BASE> WritableFloatChunk<ATTR> resetFromChunk(WritableChunk<ATTR> other, int offset, int capacity) {
        return resetFromTypedChunk(other.asWritableFloatChunk(), offset, capacity);
    }

    @Override
    public final <ATTR extends ATTR_BASE> WritableFloatChunk<ATTR> resetFromArray(Object array, int offset, int capacity) {
        final float[] typedArray = (float[])array;
        return resetFromTypedArray(typedArray, offset, capacity);
    }

    public final <ATTR extends ATTR_BASE> WritableFloatChunk<ATTR> resetFromArray(Object array) {
        final float[] typedArray = (float[])array;
        return resetFromTypedArray(typedArray, 0, typedArray.length);
    }

    @Override
    public final <ATTR extends ATTR_BASE> WritableFloatChunk<ATTR> clear() {
        return resetFromArray(ArrayTypeUtils.EMPTY_FLOAT_ARRAY, 0, 0);
    }

    public final <ATTR extends ATTR_BASE> WritableFloatChunk<ATTR> resetFromTypedChunk(WritableFloatChunk<ATTR> other, int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(other.size, offset, capacity);
        return resetFromTypedArray(other.data, other.offset + offset, capacity);
    }

    public final <ATTR extends ATTR_BASE> WritableFloatChunk<ATTR> resetFromTypedArray(float[] data, int offset, int capacity) {
        ChunkHelpers.checkArrayArgs(data.length, offset, capacity);
        this.data = data;
        this.offset = offset;
        this.capacity = capacity;
        this.size = capacity;
        //noinspection unchecked
        return this;
    }

    @Override
    public final void close() {
        MultiChunkPool.forThisThread().getFloatChunkPool().giveResettableWritableFloatChunk(this);
    }
}
