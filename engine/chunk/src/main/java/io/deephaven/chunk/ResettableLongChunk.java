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
 * {@link ResettableReadOnlyChunk} implementation for long data.
 */
public class ResettableLongChunk<ATTR_UPPER extends Any>
        extends LongChunk<ATTR_UPPER>
        implements ResettableReadOnlyChunk<ATTR_UPPER> {

    public static <ATTR_BASE extends Any> ResettableLongChunk<ATTR_BASE> makeResettableChunk() {
        if (POOL_RESETTABLE_CHUNKS) {
            return MultiChunkPool.forThisThread().takeResettableLongChunk();
        }
        return new ResettableLongChunk<>();
    }

    public static <ATTR_BASE extends Any> ResettableLongChunk<ATTR_BASE> makeResettableChunkForPool() {
        return new ResettableLongChunk<>() {
            @Override
            public void close() {
                MultiChunkPool.forThisThread().giveResettableLongChunk(this);
            }
        };
    }

    private ResettableLongChunk(long[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    private ResettableLongChunk() {
        this(ArrayTypeUtils.EMPTY_LONG_ARRAY, 0, 0);
    }

    @Override
    public ResettableLongChunk<ATTR_UPPER> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new ResettableLongChunk<>(data, this.offset + offset, capacity);
    }

    @Override
    public <ATTR extends ATTR_UPPER> LongChunk<ATTR> resetFromChunk(Chunk<? extends ATTR> other, int offset,
            int capacity) {
        return resetFromTypedChunk(other.asLongChunk(), offset, capacity);
    }

    @Override
    public <ATTR extends ATTR_UPPER> LongChunk<ATTR> resetFromArray(Object array, int offset, int capacity) {
        final long[] typedArray = (long[]) array;
        return resetFromTypedArray(typedArray, offset, capacity);
    }

    @Override
    public <ATTR extends ATTR_UPPER> LongChunk<ATTR> resetFromArray(Object array) {
        final long[] typedArray = (long[]) array;
        return resetFromTypedArray(typedArray, 0, typedArray.length);
    }

    @Override
    public <ATTR extends ATTR_UPPER> LongChunk<ATTR> clear() {
        return resetFromArray(ArrayTypeUtils.EMPTY_LONG_ARRAY, 0, 0);
    }

    public <ATTR extends ATTR_UPPER> LongChunk<ATTR> resetFromTypedChunk(LongChunk<? extends ATTR> other, int offset,
            int capacity) {
        ChunkHelpers.checkSliceArgs(other.size, offset, capacity);
        return resetFromTypedArray(other.data, other.offset + offset, capacity);
    }

    public <ATTR extends ATTR_UPPER> LongChunk<ATTR> resetFromTypedArray(long[] data, int offset, int capacity) {
        ChunkHelpers.checkArrayArgs(data.length, offset, capacity);
        this.data = data;
        this.offset = offset;
        this.capacity = capacity;
        this.size = capacity;
        return LongChunk.downcast(this);
    }

    @Override
    public void close() {}
}
