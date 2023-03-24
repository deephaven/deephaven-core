/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ResettableCharChunk and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.util.pools.MultiChunkPool;
import io.deephaven.util.type.ArrayTypeUtils;

import static io.deephaven.chunk.util.pools.ChunkPoolConstants.POOL_RESETTABLE_CHUNKS;

/**
 * {@link ResettableReadOnlyChunk} implementation for int data.
 */
@SuppressWarnings("rawtypes")
public final class ResettableIntChunk<ATTR_UPPER extends Any> extends IntChunk implements ResettableReadOnlyChunk<ATTR_UPPER> {

    public static <ATTR_BASE extends Any> ResettableIntChunk<ATTR_BASE> makeResettableChunk() {
        if (POOL_RESETTABLE_CHUNKS) {
            return MultiChunkPool.forThisThread().getIntChunkPool().takeResettableIntChunk();
        }
        return new ResettableIntChunk<>();
    }

    public static <ATTR_BASE extends Any> ResettableIntChunk<ATTR_BASE> makeResettableChunkForPool() {
        return new ResettableIntChunk<>();
    }

    private ResettableIntChunk(int[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    private ResettableIntChunk() {
        this(ArrayTypeUtils.EMPTY_INT_ARRAY, 0, 0);
    }

    @Override
    public ResettableIntChunk slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new ResettableIntChunk<>(data, this.offset + offset, capacity);
    }

    @Override
    public <ATTR extends ATTR_UPPER> IntChunk<ATTR> resetFromChunk(Chunk<? extends ATTR> other, int offset, int capacity) {
        return resetFromTypedChunk(other.asIntChunk(), offset, capacity);
    }

    @Override
    public <ATTR extends ATTR_UPPER> IntChunk<ATTR> resetFromArray(Object array, int offset, int capacity) {
        final int[] typedArray = (int[]) array;
        return resetFromTypedArray(typedArray, offset, capacity);
    }

    @Override
    public <ATTR extends ATTR_UPPER> IntChunk<ATTR> resetFromArray(Object array) {
        final int[] typedArray = (int[]) array;
        return resetFromTypedArray(typedArray, 0, typedArray.length);
    }

    @Override
    public <ATTR extends ATTR_UPPER> IntChunk<ATTR> clear() {
        return resetFromArray(ArrayTypeUtils.EMPTY_INT_ARRAY, 0, 0);
    }

    public <ATTR extends ATTR_UPPER> IntChunk<ATTR> resetFromTypedChunk(IntChunk<? extends ATTR> other, int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(other.size, offset, capacity);
        return resetFromTypedArray(other.data, other.offset + offset, capacity);
    }

    public <ATTR extends ATTR_UPPER> IntChunk<ATTR> resetFromTypedArray(int[] data, int offset, int capacity) {
        ChunkHelpers.checkArrayArgs(data.length, offset, capacity);
        this.data = data;
        this.offset = offset;
        this.capacity = capacity;
        this.size = capacity;
        // noinspection unchecked
        return this;
    }

    @Override
    public void close() {
        if (POOL_RESETTABLE_CHUNKS) {
            MultiChunkPool.forThisThread().getIntChunkPool().giveResettableIntChunk(this);
        }
    }
}
