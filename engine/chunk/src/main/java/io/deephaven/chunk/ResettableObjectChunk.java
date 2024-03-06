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
 * {@link ResettableReadOnlyChunk} implementation for Object data.
 */
public class ResettableObjectChunk<T, ATTR_UPPER extends Any>
        extends ObjectChunk<T, ATTR_UPPER>
        implements ResettableReadOnlyChunk<ATTR_UPPER> {

    public static <T, ATTR_BASE extends Any> ResettableObjectChunk<T, ATTR_BASE> makeResettableChunk() {
        if (POOL_RESETTABLE_CHUNKS) {
            return MultiChunkPool.forThisThread().takeResettableObjectChunk();
        }
        return new ResettableObjectChunk<>();
    }

    public static <T, ATTR_BASE extends Any> ResettableObjectChunk<T, ATTR_BASE> makeResettableChunkForPool() {
        return new ResettableObjectChunk<>() {
            @Override
            public void close() {
                MultiChunkPool.forThisThread().giveResettableObjectChunk(this);
            }
        };
    }

    private ResettableObjectChunk(T[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    private ResettableObjectChunk() {
        //noinspection unchecked
        this((T[])ArrayTypeUtils.EMPTY_OBJECT_ARRAY, 0, 0);
    }

    @Override
    public ResettableObjectChunk<T, ATTR_UPPER> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new ResettableObjectChunk<>(data, this.offset + offset, capacity);
    }

    @Override
    public <ATTR extends ATTR_UPPER> ObjectChunk<T, ATTR> resetFromChunk(Chunk<? extends ATTR> other, int offset,
            int capacity) {
        return resetFromTypedChunk(other.asObjectChunk(), offset, capacity);
    }

    @Override
    public <ATTR extends ATTR_UPPER> ObjectChunk<T, ATTR> resetFromArray(Object array, int offset, int capacity) {
        //noinspection unchecked
        final T[] typedArray = (T[]) array;
        return resetFromTypedArray(typedArray, offset, capacity);
    }

    @Override
    public <ATTR extends ATTR_UPPER> ObjectChunk<T, ATTR> resetFromArray(Object array) {
        //noinspection unchecked
        final T[] typedArray = (T[]) array;
        return resetFromTypedArray(typedArray, 0, typedArray.length);
    }

    @Override
    public <ATTR extends ATTR_UPPER> ObjectChunk<T, ATTR> clear() {
        return resetFromArray(ArrayTypeUtils.EMPTY_OBJECT_ARRAY, 0, 0);
    }

    public <ATTR extends ATTR_UPPER> ObjectChunk<T, ATTR> resetFromTypedChunk(ObjectChunk<T, ? extends ATTR> other, int offset,
            int capacity) {
        ChunkHelpers.checkSliceArgs(other.size, offset, capacity);
        return resetFromTypedArray(other.data, other.offset + offset, capacity);
    }

    public <ATTR extends ATTR_UPPER> ObjectChunk<T, ATTR> resetFromTypedArray(T[] data, int offset, int capacity) {
        ChunkHelpers.checkArrayArgs(data.length, offset, capacity);
        this.data = data;
        this.offset = offset;
        this.capacity = capacity;
        this.size = capacity;
        return ObjectChunk.downcast(this);
    }

    @Override
    public void close() {}
}
