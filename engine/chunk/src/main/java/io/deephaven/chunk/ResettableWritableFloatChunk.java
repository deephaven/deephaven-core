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
 * {@link ResettableWritableChunk} implementation for float data.
 */
public class ResettableWritableFloatChunk<ATTR_BASE extends Any>
        extends WritableFloatChunk<ATTR_BASE>
        implements ResettableWritableChunk<ATTR_BASE> {

    public static <ATTR_BASE extends Any> ResettableWritableFloatChunk<ATTR_BASE> makeResettableChunk() {
        if (POOL_RESETTABLE_CHUNKS) {
            return MultiChunkPool.forThisThread().takeResettableWritableFloatChunk();
        }
        return new ResettableWritableFloatChunk<>();
    }

    public static <ATTR_BASE extends Any> ResettableWritableFloatChunk<ATTR_BASE> makeResettableChunkForPool() {
        return new ResettableWritableFloatChunk<>() {
            @Override
            public void close() {
                MultiChunkPool.forThisThread().giveResettableWritableFloatChunk(this);
            }
        };
    }

    private ResettableWritableFloatChunk(float[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    private ResettableWritableFloatChunk() {
        this(ArrayTypeUtils.EMPTY_FLOAT_ARRAY, 0, 0);
    }

    @Override
    public ResettableWritableFloatChunk<ATTR_BASE> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new ResettableWritableFloatChunk<>(data, this.offset + offset, capacity);
    }

    @Override
    public <ATTR extends ATTR_BASE> WritableFloatChunk<ATTR> resetFromChunk(WritableChunk<ATTR> other, int offset,
            int capacity) {
        return resetFromTypedChunk(other.asWritableFloatChunk(), offset, capacity);
    }

    @Override
    public <ATTR extends ATTR_BASE> WritableFloatChunk<ATTR> resetFromArray(Object array, int offset, int capacity) {
        final float[] typedArray = (float[]) array;
        return resetFromTypedArray(typedArray, offset, capacity);
    }

    public <ATTR extends ATTR_BASE> WritableFloatChunk<ATTR> resetFromArray(Object array) {
        final float[] typedArray = (float[]) array;
        return resetFromTypedArray(typedArray, 0, typedArray.length);
    }

    @Override
    public <ATTR extends ATTR_BASE> WritableFloatChunk<ATTR> clear() {
        return resetFromArray(ArrayTypeUtils.EMPTY_FLOAT_ARRAY, 0, 0);
    }

    public <ATTR extends ATTR_BASE> WritableFloatChunk<ATTR> resetFromTypedChunk(WritableFloatChunk<ATTR> other,
            int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(other.size, offset, capacity);
        return resetFromTypedArray(other.data, other.offset + offset, capacity);
    }

    public <ATTR extends ATTR_BASE> WritableFloatChunk<ATTR> resetFromTypedArray(float[] data, int offset, int capacity) {
        ChunkHelpers.checkArrayArgs(data.length, offset, capacity);
        this.data = data;
        this.offset = offset;
        this.capacity = capacity;
        this.size = capacity;
        // noinspection unchecked
        return (WritableFloatChunk<ATTR>) this;
    }
}
