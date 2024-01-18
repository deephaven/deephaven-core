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
 * {@link ResettableWritableChunk} implementation for boolean data.
 */
public class ResettableWritableBooleanChunk<ATTR_BASE extends Any>
        extends WritableBooleanChunk<ATTR_BASE>
        implements ResettableWritableChunk<ATTR_BASE> {

    public static <ATTR_BASE extends Any> ResettableWritableBooleanChunk<ATTR_BASE> makeResettableChunk() {
        if (POOL_RESETTABLE_CHUNKS) {
            return MultiChunkPool.forThisThread().takeResettableWritableBooleanChunk();
        }
        return new ResettableWritableBooleanChunk<>();
    }

    public static <ATTR_BASE extends Any> ResettableWritableBooleanChunk<ATTR_BASE> makeResettableChunkForPool() {
        return new ResettableWritableBooleanChunk<>() {
            @Override
            public void close() {
                MultiChunkPool.forThisThread().giveResettableWritableBooleanChunk(this);
            }
        };
    }

    private ResettableWritableBooleanChunk(boolean[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    private ResettableWritableBooleanChunk() {
        this(ArrayTypeUtils.EMPTY_BOOLEAN_ARRAY, 0, 0);
    }

    @Override
    public ResettableWritableBooleanChunk<ATTR_BASE> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new ResettableWritableBooleanChunk<>(data, this.offset + offset, capacity);
    }

    @Override
    public <ATTR extends ATTR_BASE> WritableBooleanChunk<ATTR> resetFromChunk(WritableChunk<ATTR> other, int offset,
            int capacity) {
        return resetFromTypedChunk(other.asWritableBooleanChunk(), offset, capacity);
    }

    @Override
    public <ATTR extends ATTR_BASE> WritableBooleanChunk<ATTR> resetFromArray(Object array, int offset, int capacity) {
        final boolean[] typedArray = (boolean[]) array;
        return resetFromTypedArray(typedArray, offset, capacity);
    }

    public <ATTR extends ATTR_BASE> WritableBooleanChunk<ATTR> resetFromArray(Object array) {
        final boolean[] typedArray = (boolean[]) array;
        return resetFromTypedArray(typedArray, 0, typedArray.length);
    }

    @Override
    public <ATTR extends ATTR_BASE> WritableBooleanChunk<ATTR> clear() {
        return resetFromArray(ArrayTypeUtils.EMPTY_BOOLEAN_ARRAY, 0, 0);
    }

    public <ATTR extends ATTR_BASE> WritableBooleanChunk<ATTR> resetFromTypedChunk(WritableBooleanChunk<ATTR> other,
            int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(other.size, offset, capacity);
        return resetFromTypedArray(other.data, other.offset + offset, capacity);
    }

    public <ATTR extends ATTR_BASE> WritableBooleanChunk<ATTR> resetFromTypedArray(boolean[] data, int offset, int capacity) {
        ChunkHelpers.checkArrayArgs(data.length, offset, capacity);
        this.data = data;
        this.offset = offset;
        this.capacity = capacity;
        this.size = capacity;
        // noinspection unchecked
        return (WritableBooleanChunk<ATTR>) this;
    }
}
