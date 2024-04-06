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
 * {@link ResettableWritableChunk} implementation for Object data.
 */
public class ResettableWritableObjectChunk<T, ATTR_BASE extends Any>
        extends WritableObjectChunk<T, ATTR_BASE>
        implements ResettableWritableChunk<ATTR_BASE> {

    public static <T, ATTR_BASE extends Any> ResettableWritableObjectChunk<T, ATTR_BASE> makeResettableChunk() {
        if (POOL_RESETTABLE_CHUNKS) {
            return MultiChunkPool.forThisThread().takeResettableWritableObjectChunk();
        }
        return new ResettableWritableObjectChunk<>();
    }

    public static <T, ATTR_BASE extends Any> ResettableWritableObjectChunk<T, ATTR_BASE> makeResettableChunkForPool() {
        return new ResettableWritableObjectChunk<>() {
            @Override
            public void close() {
                MultiChunkPool.forThisThread().giveResettableWritableObjectChunk(this);
            }
        };
    }

    private ResettableWritableObjectChunk(T[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    private ResettableWritableObjectChunk() {
        //noinspection unchecked
        this((T[])ArrayTypeUtils.EMPTY_OBJECT_ARRAY, 0, 0);
    }

    @Override
    public ResettableWritableObjectChunk<T, ATTR_BASE> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new ResettableWritableObjectChunk<>(data, this.offset + offset, capacity);
    }

    @Override
    public <ATTR extends ATTR_BASE> WritableObjectChunk<T, ATTR> resetFromChunk(WritableChunk<ATTR> other, int offset,
            int capacity) {
        return resetFromTypedChunk(other.asWritableObjectChunk(), offset, capacity);
    }

    @Override
    public <ATTR extends ATTR_BASE> WritableObjectChunk<T, ATTR> resetFromArray(Object array, int offset, int capacity) {
        //noinspection unchecked
        final T[] typedArray = (T[]) array;
        return resetFromTypedArray(typedArray, offset, capacity);
    }

    public <ATTR extends ATTR_BASE> WritableObjectChunk<T, ATTR> resetFromArray(Object array) {
        //noinspection unchecked
        final T[] typedArray = (T[]) array;
        return resetFromTypedArray(typedArray, 0, typedArray.length);
    }

    @Override
    public <ATTR extends ATTR_BASE> WritableObjectChunk<T, ATTR> clear() {
        return resetFromArray(ArrayTypeUtils.EMPTY_OBJECT_ARRAY, 0, 0);
    }

    public <ATTR extends ATTR_BASE> WritableObjectChunk<T, ATTR> resetFromTypedChunk(WritableObjectChunk<T, ATTR> other,
            int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(other.size, offset, capacity);
        return resetFromTypedArray(other.data, other.offset + offset, capacity);
    }

    public <ATTR extends ATTR_BASE> WritableObjectChunk<T, ATTR> resetFromTypedArray(T[] data, int offset, int capacity) {
        ChunkHelpers.checkArrayArgs(data.length, offset, capacity);
        this.data = data;
        this.offset = offset;
        this.capacity = capacity;
        this.size = capacity;
        // noinspection unchecked
        return (WritableObjectChunk<T, ATTR>) this;
    }
}
