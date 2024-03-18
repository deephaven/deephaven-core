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
 * {@link ResettableWritableChunk} implementation for short data.
 */
public class ResettableWritableShortChunk<ATTR_BASE extends Any>
        extends WritableShortChunk<ATTR_BASE>
        implements ResettableWritableChunk<ATTR_BASE> {

    public static <ATTR_BASE extends Any> ResettableWritableShortChunk<ATTR_BASE> makeResettableChunk() {
        if (POOL_RESETTABLE_CHUNKS) {
            return MultiChunkPool.forThisThread().takeResettableWritableShortChunk();
        }
        return new ResettableWritableShortChunk<>();
    }

    public static <ATTR_BASE extends Any> ResettableWritableShortChunk<ATTR_BASE> makeResettableChunkForPool() {
        return new ResettableWritableShortChunk<>() {
            @Override
            public void close() {
                MultiChunkPool.forThisThread().giveResettableWritableShortChunk(this);
            }
        };
    }

    private ResettableWritableShortChunk(short[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    private ResettableWritableShortChunk() {
        this(ArrayTypeUtils.EMPTY_SHORT_ARRAY, 0, 0);
    }

    @Override
    public ResettableWritableShortChunk<ATTR_BASE> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new ResettableWritableShortChunk<>(data, this.offset + offset, capacity);
    }

    @Override
    public <ATTR extends ATTR_BASE> WritableShortChunk<ATTR> resetFromChunk(WritableChunk<ATTR> other, int offset,
            int capacity) {
        return resetFromTypedChunk(other.asWritableShortChunk(), offset, capacity);
    }

    @Override
    public <ATTR extends ATTR_BASE> WritableShortChunk<ATTR> resetFromArray(Object array, int offset, int capacity) {
        final short[] typedArray = (short[]) array;
        return resetFromTypedArray(typedArray, offset, capacity);
    }

    public <ATTR extends ATTR_BASE> WritableShortChunk<ATTR> resetFromArray(Object array) {
        final short[] typedArray = (short[]) array;
        return resetFromTypedArray(typedArray, 0, typedArray.length);
    }

    @Override
    public <ATTR extends ATTR_BASE> WritableShortChunk<ATTR> clear() {
        return resetFromArray(ArrayTypeUtils.EMPTY_SHORT_ARRAY, 0, 0);
    }

    public <ATTR extends ATTR_BASE> WritableShortChunk<ATTR> resetFromTypedChunk(WritableShortChunk<ATTR> other,
            int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(other.size, offset, capacity);
        return resetFromTypedArray(other.data, other.offset + offset, capacity);
    }

    public <ATTR extends ATTR_BASE> WritableShortChunk<ATTR> resetFromTypedArray(short[] data, int offset, int capacity) {
        ChunkHelpers.checkArrayArgs(data.length, offset, capacity);
        this.data = data;
        this.offset = offset;
        this.capacity = capacity;
        this.size = capacity;
        // noinspection unchecked
        return (WritableShortChunk<ATTR>) this;
    }
}
