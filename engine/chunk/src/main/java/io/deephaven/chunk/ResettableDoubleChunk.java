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
 * {@link ResettableReadOnlyChunk} implementation for double data.
 */
public class ResettableDoubleChunk<ATTR_UPPER extends Any>
        extends DoubleChunk<ATTR_UPPER>
        implements ResettableReadOnlyChunk<ATTR_UPPER> {

    public static <ATTR_BASE extends Any> ResettableDoubleChunk<ATTR_BASE> makeResettableChunk() {
        if (POOL_RESETTABLE_CHUNKS) {
            return MultiChunkPool.forThisThread().takeResettableDoubleChunk();
        }
        return new ResettableDoubleChunk<>();
    }

    public static <ATTR_BASE extends Any> ResettableDoubleChunk<ATTR_BASE> makeResettableChunkForPool() {
        return new ResettableDoubleChunk<>() {
            @Override
            public void close() {
                MultiChunkPool.forThisThread().giveResettableDoubleChunk(this);
            }
        };
    }

    private ResettableDoubleChunk(double[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    private ResettableDoubleChunk() {
        this(ArrayTypeUtils.EMPTY_DOUBLE_ARRAY, 0, 0);
    }

    @Override
    public ResettableDoubleChunk<ATTR_UPPER> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new ResettableDoubleChunk<>(data, this.offset + offset, capacity);
    }

    @Override
    public <ATTR extends ATTR_UPPER> DoubleChunk<ATTR> resetFromChunk(Chunk<? extends ATTR> other, int offset,
            int capacity) {
        return resetFromTypedChunk(other.asDoubleChunk(), offset, capacity);
    }

    @Override
    public <ATTR extends ATTR_UPPER> DoubleChunk<ATTR> resetFromArray(Object array, int offset, int capacity) {
        final double[] typedArray = (double[]) array;
        return resetFromTypedArray(typedArray, offset, capacity);
    }

    @Override
    public <ATTR extends ATTR_UPPER> DoubleChunk<ATTR> resetFromArray(Object array) {
        final double[] typedArray = (double[]) array;
        return resetFromTypedArray(typedArray, 0, typedArray.length);
    }

    @Override
    public <ATTR extends ATTR_UPPER> DoubleChunk<ATTR> clear() {
        return resetFromArray(ArrayTypeUtils.EMPTY_DOUBLE_ARRAY, 0, 0);
    }

    public <ATTR extends ATTR_UPPER> DoubleChunk<ATTR> resetFromTypedChunk(DoubleChunk<? extends ATTR> other, int offset,
            int capacity) {
        ChunkHelpers.checkSliceArgs(other.size, offset, capacity);
        return resetFromTypedArray(other.data, other.offset + offset, capacity);
    }

    public <ATTR extends ATTR_UPPER> DoubleChunk<ATTR> resetFromTypedArray(double[] data, int offset, int capacity) {
        ChunkHelpers.checkArrayArgs(data.length, offset, capacity);
        this.data = data;
        this.offset = offset;
        this.capacity = capacity;
        this.size = capacity;
        return DoubleChunk.downcast(this);
    }

    @Override
    public void close() {}
}
