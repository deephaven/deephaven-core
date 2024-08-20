//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit ResettableCharChunk and run "./gradlew replicateSourcesAndChunks" to regenerate
//
// @formatter:off
package io.deephaven.chunk;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.util.pools.MultiChunkPool;
import io.deephaven.util.type.ArrayTypeUtils;

import static io.deephaven.chunk.util.pools.ChunkPoolConstants.POOL_RESETTABLE_CHUNKS;

/**
 * {@link ResettableReadOnlyChunk} implementation for short data.
 */
public class ResettableShortChunk<ATTR_UPPER extends Any>
        extends ShortChunk<ATTR_UPPER>
        implements ResettableReadOnlyChunk<ATTR_UPPER> {

    public static <ATTR_BASE extends Any> ResettableShortChunk<ATTR_BASE> makeResettableChunk() {
        if (POOL_RESETTABLE_CHUNKS) {
            return MultiChunkPool.forThisThread().takeResettableShortChunk();
        }
        return new ResettableShortChunk<>();
    }

    public static <ATTR_BASE extends Any> ResettableShortChunk<ATTR_BASE> makeResettableChunkForPool() {
        return new ResettableShortChunk<>() {
            @Override
            public void close() {
                MultiChunkPool.forThisThread().giveResettableShortChunk(this);
            }
        };
    }

    private ResettableShortChunk(short[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    private ResettableShortChunk() {
        this(ArrayTypeUtils.EMPTY_SHORT_ARRAY, 0, 0);
    }

    @Override
    public ResettableShortChunk<ATTR_UPPER> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new ResettableShortChunk<>(data, this.offset + offset, capacity);
    }

    @Override
    public <ATTR extends ATTR_UPPER> ShortChunk<ATTR> resetFromChunk(Chunk<? extends ATTR> other, int offset,
            int capacity) {
        return resetFromTypedChunk(other.asShortChunk(), offset, capacity);
    }

    @Override
    public <ATTR extends ATTR_UPPER> ShortChunk<ATTR> resetFromArray(Object array, int offset, int capacity) {
        final short[] typedArray = (short[]) array;
        return resetFromTypedArray(typedArray, offset, capacity);
    }

    @Override
    public <ATTR extends ATTR_UPPER> ShortChunk<ATTR> resetFromArray(Object array) {
        final short[] typedArray = (short[]) array;
        return resetFromTypedArray(typedArray, 0, typedArray.length);
    }

    @Override
    public <ATTR extends ATTR_UPPER> ShortChunk<ATTR> clear() {
        return resetFromArray(ArrayTypeUtils.EMPTY_SHORT_ARRAY, 0, 0);
    }

    public <ATTR extends ATTR_UPPER> ShortChunk<ATTR> resetFromTypedChunk(ShortChunk<? extends ATTR> other, int offset,
            int capacity) {
        ChunkHelpers.checkSliceArgs(other.size, offset, capacity);
        return resetFromTypedArray(other.data, other.offset + offset, capacity);
    }

    public <ATTR extends ATTR_UPPER> ShortChunk<ATTR> resetFromTypedArray(short[] data, int offset, int capacity) {
        ChunkHelpers.checkArrayArgs(data.length, offset, capacity);
        this.data = data;
        this.offset = offset;
        this.capacity = capacity;
        this.size = capacity;
        return ShortChunk.downcast(this);
    }

    @Override
    public void close() {}
}
