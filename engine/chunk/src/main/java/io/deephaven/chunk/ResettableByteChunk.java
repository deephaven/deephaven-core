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
 * {@link ResettableReadOnlyChunk} implementation for byte data.
 */
public class ResettableByteChunk<ATTR_UPPER extends Any>
        extends ByteChunk<ATTR_UPPER>
        implements ResettableReadOnlyChunk<ATTR_UPPER> {

    public static <ATTR_BASE extends Any> ResettableByteChunk<ATTR_BASE> makeResettableChunk() {
        if (POOL_RESETTABLE_CHUNKS) {
            return MultiChunkPool.forThisThread().takeResettableByteChunk();
        }
        return new ResettableByteChunk<>();
    }

    public static <ATTR_BASE extends Any> ResettableByteChunk<ATTR_BASE> makeResettableChunkForPool() {
        return new ResettableByteChunk<>() {
            @Override
            public void close() {
                MultiChunkPool.forThisThread().giveResettableByteChunk(this);
            }
        };
    }

    private ResettableByteChunk(byte[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    private ResettableByteChunk() {
        this(ArrayTypeUtils.EMPTY_BYTE_ARRAY, 0, 0);
    }

    @Override
    public ResettableByteChunk<ATTR_UPPER> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new ResettableByteChunk<>(data, this.offset + offset, capacity);
    }

    @Override
    public <ATTR extends ATTR_UPPER> ByteChunk<ATTR> resetFromChunk(Chunk<? extends ATTR> other, int offset,
            int capacity) {
        return resetFromTypedChunk(other.asByteChunk(), offset, capacity);
    }

    @Override
    public <ATTR extends ATTR_UPPER> ByteChunk<ATTR> resetFromArray(Object array, int offset, int capacity) {
        final byte[] typedArray = (byte[]) array;
        return resetFromTypedArray(typedArray, offset, capacity);
    }

    @Override
    public <ATTR extends ATTR_UPPER> ByteChunk<ATTR> resetFromArray(Object array) {
        final byte[] typedArray = (byte[]) array;
        return resetFromTypedArray(typedArray, 0, typedArray.length);
    }

    @Override
    public <ATTR extends ATTR_UPPER> ByteChunk<ATTR> clear() {
        return resetFromArray(ArrayTypeUtils.EMPTY_BYTE_ARRAY, 0, 0);
    }

    public <ATTR extends ATTR_UPPER> ByteChunk<ATTR> resetFromTypedChunk(ByteChunk<? extends ATTR> other, int offset,
            int capacity) {
        ChunkHelpers.checkSliceArgs(other.size, offset, capacity);
        return resetFromTypedArray(other.data, other.offset + offset, capacity);
    }

    public <ATTR extends ATTR_UPPER> ByteChunk<ATTR> resetFromTypedArray(byte[] data, int offset, int capacity) {
        ChunkHelpers.checkArrayArgs(data.length, offset, capacity);
        this.data = data;
        this.offset = offset;
        this.capacity = capacity;
        this.size = capacity;
        return ByteChunk.downcast(this);
    }

    @Override
    public void close() {}
}
