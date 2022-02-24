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
 * {@link ResettableReadOnlyChunk} implementation for short data.
 */
public final class ResettableShortChunk<ATTR_UPPER extends Any> extends ShortChunk implements ResettableReadOnlyChunk<ATTR_UPPER> {

    public static <ATTR_BASE extends Any> ResettableShortChunk<ATTR_BASE> makeResettableChunk() {
        return MultiChunkPool.forThisThread().getShortChunkPool().takeResettableShortChunk();
    }

    public static <ATTR_BASE extends Any> ResettableShortChunk<ATTR_BASE> makeResettableChunkForPool() {
        return new ResettableShortChunk<>();
    }

    private ResettableShortChunk(short[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    private ResettableShortChunk() {
        this(ArrayTypeUtils.EMPTY_SHORT_ARRAY, 0, 0);
    }

    @Override
    public final ResettableShortChunk slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new ResettableShortChunk(data, this.offset + offset, capacity);
    }

    @Override
    public final <ATTR extends ATTR_UPPER> ShortChunk<ATTR> resetFromChunk(Chunk<? extends ATTR> other, int offset, int capacity) {
        return resetFromTypedChunk(other.asShortChunk(), offset, capacity);
    }

    @Override
    public final <ATTR extends ATTR_UPPER> ShortChunk<ATTR> resetFromArray(Object array, int offset, int capacity) {
        final short[] typedArray = (short[])array;
        return resetFromTypedArray(typedArray, offset, capacity);
    }

    @Override
    public final <ATTR extends ATTR_UPPER> ShortChunk<ATTR> resetFromArray(Object array) {
        final short[] typedArray = (short[])array;
        return resetFromTypedArray(typedArray, 0, typedArray.length);
    }

    @Override
    public final <ATTR extends ATTR_UPPER> ShortChunk<ATTR> clear() {
        return resetFromArray(ArrayTypeUtils.EMPTY_SHORT_ARRAY, 0, 0);
    }

    public final <ATTR extends ATTR_UPPER> ShortChunk<ATTR> resetFromTypedChunk(ShortChunk<? extends ATTR> other, int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(other.size, offset, capacity);
        return resetFromTypedArray(other.data, other.offset + offset, capacity);
    }

    public final <ATTR extends ATTR_UPPER> ShortChunk<ATTR> resetFromTypedArray(short[] data, int offset, int capacity) {
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
        MultiChunkPool.forThisThread().getShortChunkPool().giveResettableShortChunk(this);
    }
}
