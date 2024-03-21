//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit ResettableWritableCharChunk and run "./gradlew replicateSourcesAndChunks" to regenerate
//
// @formatter:off
package io.deephaven.chunk;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.util.pools.MultiChunkPool;
import io.deephaven.util.type.ArrayTypeUtils;

import static io.deephaven.chunk.util.pools.ChunkPoolConstants.POOL_RESETTABLE_CHUNKS;

/**
 * {@link ResettableWritableChunk} implementation for int data.
 */
public class ResettableWritableIntChunk<ATTR_BASE extends Any>
        extends WritableIntChunk<ATTR_BASE>
        implements ResettableWritableChunk<ATTR_BASE> {

    public static <ATTR_BASE extends Any> ResettableWritableIntChunk<ATTR_BASE> makeResettableChunk() {
        if (POOL_RESETTABLE_CHUNKS) {
            return MultiChunkPool.forThisThread().takeResettableWritableIntChunk();
        }
        return new ResettableWritableIntChunk<>();
    }

    public static <ATTR_BASE extends Any> ResettableWritableIntChunk<ATTR_BASE> makeResettableChunkForPool() {
        return new ResettableWritableIntChunk<>() {
            @Override
            public void close() {
                MultiChunkPool.forThisThread().giveResettableWritableIntChunk(this);
            }
        };
    }

    private ResettableWritableIntChunk(int[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    private ResettableWritableIntChunk() {
        this(ArrayTypeUtils.EMPTY_INT_ARRAY, 0, 0);
    }

    @Override
    public ResettableWritableIntChunk<ATTR_BASE> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new ResettableWritableIntChunk<>(data, this.offset + offset, capacity);
    }

    @Override
    public <ATTR extends ATTR_BASE> WritableIntChunk<ATTR> resetFromChunk(WritableChunk<ATTR> other, int offset,
            int capacity) {
        return resetFromTypedChunk(other.asWritableIntChunk(), offset, capacity);
    }

    @Override
    public <ATTR extends ATTR_BASE> WritableIntChunk<ATTR> resetFromArray(Object array, int offset, int capacity) {
        final int[] typedArray = (int[]) array;
        return resetFromTypedArray(typedArray, offset, capacity);
    }

    public <ATTR extends ATTR_BASE> WritableIntChunk<ATTR> resetFromArray(Object array) {
        final int[] typedArray = (int[]) array;
        return resetFromTypedArray(typedArray, 0, typedArray.length);
    }

    @Override
    public <ATTR extends ATTR_BASE> WritableIntChunk<ATTR> clear() {
        return resetFromArray(ArrayTypeUtils.EMPTY_INT_ARRAY, 0, 0);
    }

    public <ATTR extends ATTR_BASE> WritableIntChunk<ATTR> resetFromTypedChunk(WritableIntChunk<ATTR> other,
            int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(other.size, offset, capacity);
        return resetFromTypedArray(other.data, other.offset + offset, capacity);
    }

    public <ATTR extends ATTR_BASE> WritableIntChunk<ATTR> resetFromTypedArray(int[] data, int offset, int capacity) {
        ChunkHelpers.checkArrayArgs(data.length, offset, capacity);
        this.data = data;
        this.offset = offset;
        this.capacity = capacity;
        this.size = capacity;
        // noinspection unchecked
        return (WritableIntChunk<ATTR>) this;
    }
}
