/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ResettableCharChunk and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.util.pools.MultiChunkPool;
import io.deephaven.util.type.ArrayTypeUtils;

/**
 * {@link ResettableReadOnlyChunk} implementation for float data.
 */
public final class ResettableFloatChunk<ATTR_UPPER extends Any> extends FloatChunk implements ResettableReadOnlyChunk<ATTR_UPPER> {

    public static <ATTR_BASE extends Any> ResettableFloatChunk<ATTR_BASE> makeResettableChunk() {
        return MultiChunkPool.forThisThread().getFloatChunkPool().takeResettableFloatChunk();
    }

    public static <ATTR_BASE extends Any> ResettableFloatChunk<ATTR_BASE> makeResettableChunkForPool() {
        return new ResettableFloatChunk<>();
    }

    private ResettableFloatChunk(float[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    private ResettableFloatChunk() {
        this(ArrayTypeUtils.EMPTY_FLOAT_ARRAY, 0, 0);
    }

    @Override
    public final ResettableFloatChunk slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new ResettableFloatChunk(data, this.offset + offset, capacity);
    }

    @Override
    public final <ATTR extends ATTR_UPPER> FloatChunk<ATTR> resetFromChunk(Chunk<? extends ATTR> other, int offset, int capacity) {
        return resetFromTypedChunk(other.asFloatChunk(), offset, capacity);
    }

    @Override
    public final <ATTR extends ATTR_UPPER> FloatChunk<ATTR> resetFromArray(Object array, int offset, int capacity) {
        final float[] typedArray = (float[])array;
        return resetFromTypedArray(typedArray, offset, capacity);
    }

    @Override
    public final <ATTR extends ATTR_UPPER> FloatChunk<ATTR> resetFromArray(Object array) {
        final float[] typedArray = (float[])array;
        return resetFromTypedArray(typedArray, 0, typedArray.length);
    }

    @Override
    public final <ATTR extends ATTR_UPPER> FloatChunk<ATTR> clear() {
        return resetFromArray(ArrayTypeUtils.EMPTY_FLOAT_ARRAY, 0, 0);
    }

    public final <ATTR extends ATTR_UPPER> FloatChunk<ATTR> resetFromTypedChunk(FloatChunk<? extends ATTR> other, int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(other.size, offset, capacity);
        return resetFromTypedArray(other.data, other.offset + offset, capacity);
    }

    public final <ATTR extends ATTR_UPPER> FloatChunk<ATTR> resetFromTypedArray(float[] data, int offset, int capacity) {
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
        MultiChunkPool.forThisThread().getFloatChunkPool().giveResettableFloatChunk(this);
    }
}
