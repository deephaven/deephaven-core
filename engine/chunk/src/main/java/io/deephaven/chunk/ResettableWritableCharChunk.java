//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.chunk;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.util.pools.MultiChunkPool;
import io.deephaven.util.type.ArrayTypeUtils;

import static io.deephaven.chunk.util.pools.ChunkPoolConstants.POOL_RESETTABLE_CHUNKS;

/**
 * {@link ResettableWritableChunk} implementation for char data.
 */
public class ResettableWritableCharChunk<ATTR_BASE extends Any>
        extends WritableCharChunk<ATTR_BASE>
        implements ResettableWritableChunk<ATTR_BASE> {

    public static <ATTR_BASE extends Any> ResettableWritableCharChunk<ATTR_BASE> makeResettableChunk() {
        if (POOL_RESETTABLE_CHUNKS) {
            return MultiChunkPool.forThisThread().takeResettableWritableCharChunk();
        }
        return new ResettableWritableCharChunk<>();
    }

    public static <ATTR_BASE extends Any> ResettableWritableCharChunk<ATTR_BASE> makeResettableChunkForPool() {
        return new ResettableWritableCharChunk<>() {
            @Override
            public void close() {
                MultiChunkPool.forThisThread().giveResettableWritableCharChunk(this);
            }
        };
    }

    private ResettableWritableCharChunk(char[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    private ResettableWritableCharChunk() {
        this(ArrayTypeUtils.EMPTY_CHAR_ARRAY, 0, 0);
    }

    @Override
    public ResettableWritableCharChunk<ATTR_BASE> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new ResettableWritableCharChunk<>(data, this.offset + offset, capacity);
    }

    @Override
    public <ATTR extends ATTR_BASE> WritableCharChunk<ATTR> resetFromChunk(WritableChunk<ATTR> other, int offset,
            int capacity) {
        return resetFromTypedChunk(other.asWritableCharChunk(), offset, capacity);
    }

    @Override
    public <ATTR extends ATTR_BASE> WritableCharChunk<ATTR> resetFromArray(Object array, int offset, int capacity) {
        final char[] typedArray = (char[]) array;
        return resetFromTypedArray(typedArray, offset, capacity);
    }

    public <ATTR extends ATTR_BASE> WritableCharChunk<ATTR> resetFromArray(Object array) {
        final char[] typedArray = (char[]) array;
        return resetFromTypedArray(typedArray, 0, typedArray.length);
    }

    @Override
    public <ATTR extends ATTR_BASE> WritableCharChunk<ATTR> clear() {
        return resetFromArray(ArrayTypeUtils.EMPTY_CHAR_ARRAY, 0, 0);
    }

    public <ATTR extends ATTR_BASE> WritableCharChunk<ATTR> resetFromTypedChunk(WritableCharChunk<ATTR> other,
            int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(other.size, offset, capacity);
        return resetFromTypedArray(other.data, other.offset + offset, capacity);
    }

    public <ATTR extends ATTR_BASE> WritableCharChunk<ATTR> resetFromTypedArray(char[] data, int offset, int capacity) {
        ChunkHelpers.checkArrayArgs(data.length, offset, capacity);
        this.data = data;
        this.offset = offset;
        this.capacity = capacity;
        this.size = capacity;
        // noinspection unchecked
        return (WritableCharChunk<ATTR>) this;
    }
}
