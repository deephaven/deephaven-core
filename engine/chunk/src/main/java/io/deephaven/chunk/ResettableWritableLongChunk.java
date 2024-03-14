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
 * {@link ResettableWritableChunk} implementation for long data.
 */
public class ResettableWritableLongChunk<ATTR_BASE extends Any>
        extends WritableLongChunk<ATTR_BASE>
        implements ResettableWritableChunk<ATTR_BASE> {

    public static <ATTR_BASE extends Any> ResettableWritableLongChunk<ATTR_BASE> makeResettableChunk() {
        if (POOL_RESETTABLE_CHUNKS) {
            return MultiChunkPool.forThisThread().takeResettableWritableLongChunk();
        }
        return new ResettableWritableLongChunk<>();
    }

    public static <ATTR_BASE extends Any> ResettableWritableLongChunk<ATTR_BASE> makeResettableChunkForPool() {
        return new ResettableWritableLongChunk<>() {
            @Override
            public void close() {
                MultiChunkPool.forThisThread().giveResettableWritableLongChunk(this);
            }
        };
    }

    private ResettableWritableLongChunk(long[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    private ResettableWritableLongChunk() {
        this(ArrayTypeUtils.EMPTY_LONG_ARRAY, 0, 0);
    }

    @Override
    public ResettableWritableLongChunk<ATTR_BASE> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new ResettableWritableLongChunk<>(data, this.offset + offset, capacity);
    }

    @Override
    public <ATTR extends ATTR_BASE> WritableLongChunk<ATTR> resetFromChunk(WritableChunk<ATTR> other, int offset,
            int capacity) {
        return resetFromTypedChunk(other.asWritableLongChunk(), offset, capacity);
    }

    @Override
    public <ATTR extends ATTR_BASE> WritableLongChunk<ATTR> resetFromArray(Object array, int offset, int capacity) {
        final long[] typedArray = (long[]) array;
        return resetFromTypedArray(typedArray, offset, capacity);
    }

    public <ATTR extends ATTR_BASE> WritableLongChunk<ATTR> resetFromArray(Object array) {
        final long[] typedArray = (long[]) array;
        return resetFromTypedArray(typedArray, 0, typedArray.length);
    }

    @Override
    public <ATTR extends ATTR_BASE> WritableLongChunk<ATTR> clear() {
        return resetFromArray(ArrayTypeUtils.EMPTY_LONG_ARRAY, 0, 0);
    }

    public <ATTR extends ATTR_BASE> WritableLongChunk<ATTR> resetFromTypedChunk(WritableLongChunk<ATTR> other,
            int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(other.size, offset, capacity);
        return resetFromTypedArray(other.data, other.offset + offset, capacity);
    }

    public <ATTR extends ATTR_BASE> WritableLongChunk<ATTR> resetFromTypedArray(long[] data, int offset, int capacity) {
        ChunkHelpers.checkArrayArgs(data.length, offset, capacity);
        this.data = data;
        this.offset = offset;
        this.capacity = capacity;
        this.size = capacity;
        // noinspection unchecked
        return (WritableLongChunk<ATTR>) this;
    }
}
