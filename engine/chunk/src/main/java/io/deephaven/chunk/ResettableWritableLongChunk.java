/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ResettableWritableCharChunk and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.util.pools.MultiChunkPool;
import io.deephaven.util.type.ArrayTypeUtils;

/**
 * {@link ResettableWritableChunk} implementation for long data.
 */
public final class ResettableWritableLongChunk<ATTR_BASE extends Any> extends WritableLongChunk implements ResettableWritableChunk<ATTR_BASE> {

    public static <ATTR_BASE extends Any> ResettableWritableLongChunk<ATTR_BASE> makeResettableChunk() {
        return MultiChunkPool.forThisThread().getLongChunkPool().takeResettableWritableLongChunk();
    }

    public static <ATTR_BASE extends Any> ResettableWritableLongChunk<ATTR_BASE> makeResettableChunkForPool() {
        return new ResettableWritableLongChunk<>();
    }

    private ResettableWritableLongChunk(long[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    private ResettableWritableLongChunk() {
        this(ArrayTypeUtils.EMPTY_LONG_ARRAY, 0, 0);
    }

    @Override
    public final ResettableWritableLongChunk slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new ResettableWritableLongChunk<>(data, this.offset + offset, capacity);
    }

    @Override
    public final <ATTR extends ATTR_BASE> WritableLongChunk<ATTR> resetFromChunk(WritableChunk<ATTR> other, int offset, int capacity) {
        return resetFromTypedChunk(other.asWritableLongChunk(), offset, capacity);
    }

    @Override
    public final <ATTR extends ATTR_BASE> WritableLongChunk<ATTR> resetFromArray(Object array, int offset, int capacity) {
        final long[] typedArray = (long[])array;
        return resetFromTypedArray(typedArray, offset, capacity);
    }

    public final <ATTR extends ATTR_BASE> WritableLongChunk<ATTR> resetFromArray(Object array) {
        final long[] typedArray = (long[])array;
        return resetFromTypedArray(typedArray, 0, typedArray.length);
    }

    @Override
    public final <ATTR extends ATTR_BASE> WritableLongChunk<ATTR> clear() {
        return resetFromArray(ArrayTypeUtils.EMPTY_LONG_ARRAY, 0, 0);
    }

    public final <ATTR extends ATTR_BASE> WritableLongChunk<ATTR> resetFromTypedChunk(WritableLongChunk<ATTR> other, int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(other.size, offset, capacity);
        return resetFromTypedArray(other.data, other.offset + offset, capacity);
    }

    public final <ATTR extends ATTR_BASE> WritableLongChunk<ATTR> resetFromTypedArray(long[] data, int offset, int capacity) {
        ChunkHelpers.checkArrayArgs(data.length, offset, capacity);
        this.data = data;
        this.offset = offset;
        this.capacity = capacity;
        this.size = capacity;
        //noinspection unchecked
        return this;
    }

    @Override
    public final void close() {
        MultiChunkPool.forThisThread().getLongChunkPool().giveResettableWritableLongChunk(this);
    }
}
