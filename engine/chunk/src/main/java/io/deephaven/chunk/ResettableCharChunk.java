//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.chunk;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.util.pools.MultiChunkPool;
import io.deephaven.util.type.ArrayTypeUtils;

import static io.deephaven.chunk.util.pools.ChunkPoolConstants.POOL_RESETTABLE_CHUNKS;

/**
 * {@link ResettableReadOnlyChunk} implementation for char data.
 */
public class ResettableCharChunk<ATTR_UPPER extends Any>
        extends CharChunk<ATTR_UPPER>
        implements ResettableReadOnlyChunk<ATTR_UPPER> {

    public static <ATTR_BASE extends Any> ResettableCharChunk<ATTR_BASE> makeResettableChunk() {
        if (POOL_RESETTABLE_CHUNKS) {
            return MultiChunkPool.forThisThread().takeResettableCharChunk();
        }
        return new ResettableCharChunk<>();
    }

    public static <ATTR_BASE extends Any> ResettableCharChunk<ATTR_BASE> makeResettableChunkForPool() {
        return new ResettableCharChunk<>() {
            @Override
            public void close() {
                MultiChunkPool.forThisThread().giveResettableCharChunk(this);
            }
        };
    }

    private ResettableCharChunk(char[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    private ResettableCharChunk() {
        this(ArrayTypeUtils.EMPTY_CHAR_ARRAY, 0, 0);
    }

    @Override
    public ResettableCharChunk<ATTR_UPPER> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new ResettableCharChunk<>(data, this.offset + offset, capacity);
    }

    @Override
    public <ATTR extends ATTR_UPPER> CharChunk<ATTR> resetFromChunk(Chunk<? extends ATTR> other, int offset,
            int capacity) {
        return resetFromTypedChunk(other.asCharChunk(), offset, capacity);
    }

    @Override
    public <ATTR extends ATTR_UPPER> CharChunk<ATTR> resetFromArray(Object array, int offset, int capacity) {
        final char[] typedArray = (char[]) array;
        return resetFromTypedArray(typedArray, offset, capacity);
    }

    @Override
    public <ATTR extends ATTR_UPPER> CharChunk<ATTR> resetFromArray(Object array) {
        final char[] typedArray = (char[]) array;
        return resetFromTypedArray(typedArray, 0, typedArray.length);
    }

    @Override
    public <ATTR extends ATTR_UPPER> CharChunk<ATTR> clear() {
        return resetFromArray(ArrayTypeUtils.EMPTY_CHAR_ARRAY, 0, 0);
    }

    public <ATTR extends ATTR_UPPER> CharChunk<ATTR> resetFromTypedChunk(CharChunk<? extends ATTR> other, int offset,
            int capacity) {
        ChunkHelpers.checkSliceArgs(other.size, offset, capacity);
        return resetFromTypedArray(other.data, other.offset + offset, capacity);
    }

    public <ATTR extends ATTR_UPPER> CharChunk<ATTR> resetFromTypedArray(char[] data, int offset, int capacity) {
        ChunkHelpers.checkArrayArgs(data.length, offset, capacity);
        this.data = data;
        this.offset = offset;
        this.capacity = capacity;
        this.size = capacity;
        return CharChunk.downcast(this);
    }

    @Override
    public void close() {}
}
