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
 * {@link ResettableWritableChunk} implementation for short data.
 */
public final class ResettableWritableShortChunk<ATTR_BASE extends Any> extends WritableShortChunk implements ResettableWritableChunk<ATTR_BASE> {

    public static <ATTR_BASE extends Any> ResettableWritableShortChunk<ATTR_BASE> makeResettableChunk() {
        return MultiChunkPool.forThisThread().getShortChunkPool().takeResettableWritableShortChunk();
    }

    public static <ATTR_BASE extends Any> ResettableWritableShortChunk<ATTR_BASE> makeResettableChunkForPool() {
        return new ResettableWritableShortChunk<>();
    }

    private ResettableWritableShortChunk(short[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    private ResettableWritableShortChunk() {
        this(ArrayTypeUtils.EMPTY_SHORT_ARRAY, 0, 0);
    }

    @Override
    public final ResettableWritableShortChunk slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new ResettableWritableShortChunk<>(data, this.offset + offset, capacity);
    }

    @Override
    public final <ATTR extends ATTR_BASE> WritableShortChunk<ATTR> resetFromChunk(WritableChunk<ATTR> other, int offset, int capacity) {
        return resetFromTypedChunk(other.asWritableShortChunk(), offset, capacity);
    }

    @Override
    public final <ATTR extends ATTR_BASE> WritableShortChunk<ATTR> resetFromArray(Object array, int offset, int capacity) {
        final short[] typedArray = (short[])array;
        return resetFromTypedArray(typedArray, offset, capacity);
    }

    public final <ATTR extends ATTR_BASE> WritableShortChunk<ATTR> resetFromArray(Object array) {
        final short[] typedArray = (short[])array;
        return resetFromTypedArray(typedArray, 0, typedArray.length);
    }

    @Override
    public final <ATTR extends ATTR_BASE> WritableShortChunk<ATTR> clear() {
        return resetFromArray(ArrayTypeUtils.EMPTY_SHORT_ARRAY, 0, 0);
    }

    public final <ATTR extends ATTR_BASE> WritableShortChunk<ATTR> resetFromTypedChunk(WritableShortChunk<ATTR> other, int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(other.size, offset, capacity);
        return resetFromTypedArray(other.data, other.offset + offset, capacity);
    }

    public final <ATTR extends ATTR_BASE> WritableShortChunk<ATTR> resetFromTypedArray(short[] data, int offset, int capacity) {
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
        MultiChunkPool.forThisThread().getShortChunkPool().giveResettableWritableShortChunk(this);
    }
}
