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
 * {@link ResettableReadOnlyChunk} implementation for float data.
 */
public class ResettableFloatChunk<ATTR_UPPER extends Any>
        extends FloatChunk<ATTR_UPPER>
        implements ResettableReadOnlyChunk<ATTR_UPPER> {

    public static <ATTR_BASE extends Any> ResettableFloatChunk<ATTR_BASE> makeResettableChunk() {
        if (POOL_RESETTABLE_CHUNKS) {
            return MultiChunkPool.forThisThread().takeResettableFloatChunk();
        }
        return new ResettableFloatChunk<>();
    }

    public static <ATTR_BASE extends Any> ResettableFloatChunk<ATTR_BASE> makeResettableChunkForPool() {
        return new ResettableFloatChunk<>() {
            @Override
            public void close() {
                MultiChunkPool.forThisThread().giveResettableFloatChunk(this);
            }
        };
    }

    private ResettableFloatChunk(float[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    private ResettableFloatChunk() {
        this(ArrayTypeUtils.EMPTY_FLOAT_ARRAY, 0, 0);
    }

    @Override
    public ResettableFloatChunk<ATTR_UPPER> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new ResettableFloatChunk<>(data, this.offset + offset, capacity);
    }

    @Override
    public <ATTR extends ATTR_UPPER> FloatChunk<ATTR> resetFromChunk(Chunk<? extends ATTR> other, int offset,
            int capacity) {
        return resetFromTypedChunk(other.asFloatChunk(), offset, capacity);
    }

    @Override
    public <ATTR extends ATTR_UPPER> FloatChunk<ATTR> resetFromArray(Object array, int offset, int capacity) {
        final float[] typedArray = (float[]) array;
        return resetFromTypedArray(typedArray, offset, capacity);
    }

    @Override
    public <ATTR extends ATTR_UPPER> FloatChunk<ATTR> resetFromArray(Object array) {
        final float[] typedArray = (float[]) array;
        return resetFromTypedArray(typedArray, 0, typedArray.length);
    }

    @Override
    public <ATTR extends ATTR_UPPER> FloatChunk<ATTR> clear() {
        return resetFromArray(ArrayTypeUtils.EMPTY_FLOAT_ARRAY, 0, 0);
    }

    public <ATTR extends ATTR_UPPER> FloatChunk<ATTR> resetFromTypedChunk(FloatChunk<? extends ATTR> other, int offset,
            int capacity) {
        ChunkHelpers.checkSliceArgs(other.size, offset, capacity);
        return resetFromTypedArray(other.data, other.offset + offset, capacity);
    }

    public <ATTR extends ATTR_UPPER> FloatChunk<ATTR> resetFromTypedArray(float[] data, int offset, int capacity) {
        ChunkHelpers.checkArrayArgs(data.length, offset, capacity);
        this.data = data;
        this.offset = offset;
        this.capacity = capacity;
        this.size = capacity;
        return FloatChunk.downcast(this);
    }

    @Override
    public void close() {}
}
