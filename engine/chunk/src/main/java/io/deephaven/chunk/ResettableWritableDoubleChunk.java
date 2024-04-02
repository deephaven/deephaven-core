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
 * {@link ResettableWritableChunk} implementation for double data.
 */
public class ResettableWritableDoubleChunk<ATTR_BASE extends Any>
        extends WritableDoubleChunk<ATTR_BASE>
        implements ResettableWritableChunk<ATTR_BASE> {

    public static <ATTR_BASE extends Any> ResettableWritableDoubleChunk<ATTR_BASE> makeResettableChunk() {
        if (POOL_RESETTABLE_CHUNKS) {
            return MultiChunkPool.forThisThread().takeResettableWritableDoubleChunk();
        }
        return new ResettableWritableDoubleChunk<>();
    }

    public static <ATTR_BASE extends Any> ResettableWritableDoubleChunk<ATTR_BASE> makeResettableChunkForPool() {
        return new ResettableWritableDoubleChunk<>() {
            @Override
            public void close() {
                MultiChunkPool.forThisThread().giveResettableWritableDoubleChunk(this);
            }
        };
    }

    private ResettableWritableDoubleChunk(double[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    private ResettableWritableDoubleChunk() {
        this(ArrayTypeUtils.EMPTY_DOUBLE_ARRAY, 0, 0);
    }

    @Override
    public ResettableWritableDoubleChunk<ATTR_BASE> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new ResettableWritableDoubleChunk<>(data, this.offset + offset, capacity);
    }

    @Override
    public <ATTR extends ATTR_BASE> WritableDoubleChunk<ATTR> resetFromChunk(WritableChunk<ATTR> other, int offset,
            int capacity) {
        return resetFromTypedChunk(other.asWritableDoubleChunk(), offset, capacity);
    }

    @Override
    public <ATTR extends ATTR_BASE> WritableDoubleChunk<ATTR> resetFromArray(Object array, int offset, int capacity) {
        final double[] typedArray = (double[]) array;
        return resetFromTypedArray(typedArray, offset, capacity);
    }

    public <ATTR extends ATTR_BASE> WritableDoubleChunk<ATTR> resetFromArray(Object array) {
        final double[] typedArray = (double[]) array;
        return resetFromTypedArray(typedArray, 0, typedArray.length);
    }

    @Override
    public <ATTR extends ATTR_BASE> WritableDoubleChunk<ATTR> clear() {
        return resetFromArray(ArrayTypeUtils.EMPTY_DOUBLE_ARRAY, 0, 0);
    }

    public <ATTR extends ATTR_BASE> WritableDoubleChunk<ATTR> resetFromTypedChunk(WritableDoubleChunk<ATTR> other,
            int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(other.size, offset, capacity);
        return resetFromTypedArray(other.data, other.offset + offset, capacity);
    }

    public <ATTR extends ATTR_BASE> WritableDoubleChunk<ATTR> resetFromTypedArray(double[] data, int offset, int capacity) {
        ChunkHelpers.checkArrayArgs(data.length, offset, capacity);
        this.data = data;
        this.offset = offset;
        this.capacity = capacity;
        this.size = capacity;
        // noinspection unchecked
        return (WritableDoubleChunk<ATTR>) this;
    }
}
