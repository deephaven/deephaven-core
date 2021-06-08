/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ResettableWritableCharChunk and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.sources.chunk;
import io.deephaven.db.tables.utils.ArrayUtils;
import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.util.pools.MultiChunkPool;
import io.deephaven.db.v2.utils.ChunkUtils;

/**
 * {@link ResettableWritableChunk} implementation for Object data.
 */
public final class ResettableWritableObjectChunk<T, ATTR_BASE extends Any> extends WritableObjectChunk implements ResettableWritableChunk<ATTR_BASE> {

    public static <T, ATTR_BASE extends Any> ResettableWritableObjectChunk<T, ATTR_BASE> makeResettableChunk() {
        return MultiChunkPool.forThisThread().getObjectChunkPool().takeResettableWritableObjectChunk();
    }

    public static <T, ATTR_BASE extends Any> ResettableWritableObjectChunk<T, ATTR_BASE> makeResettableChunkForPool() {
        return new ResettableWritableObjectChunk<>();
    }

    private ResettableWritableObjectChunk(T[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    private ResettableWritableObjectChunk() {
        //noinspection unchecked
        this((T[])ArrayUtils.EMPTY_OBJECT_ARRAY, 0, 0);
    }

    @Override
    public final ResettableWritableObjectChunk slice(int offset, int capacity) {
        ChunkUtils.checkSliceArgs(size, offset, capacity);
        return new ResettableWritableObjectChunk<>(data, this.offset + offset, capacity);
    }

    @Override
    public final <ATTR extends ATTR_BASE> WritableObjectChunk<T, ATTR> resetFromChunk(WritableChunk<ATTR> other, int offset, int capacity) {
        return resetFromTypedChunk(other.asWritableObjectChunk(), offset, capacity);
    }

    @Override
    public final <ATTR extends ATTR_BASE> WritableObjectChunk<T, ATTR> resetFromArray(Object array, int offset, int capacity) {
        //noinspection unchecked
        final T[] typedArray = (T[])array;
        return resetFromTypedArray(typedArray, offset, capacity);
    }

    public final <ATTR extends ATTR_BASE> WritableObjectChunk<T, ATTR> resetFromArray(Object array) {
        //noinspection unchecked
        final T[] typedArray = (T[])array;
        return resetFromTypedArray(typedArray, 0, typedArray.length);
    }

    @Override
    public final <ATTR extends ATTR_BASE> WritableObjectChunk<T, ATTR> clear() {
        return resetFromArray(ArrayUtils.EMPTY_OBJECT_ARRAY, 0, 0);
    }

    public final <ATTR extends ATTR_BASE> WritableObjectChunk<T, ATTR> resetFromTypedChunk(WritableObjectChunk<T, ATTR> other, int offset, int capacity) {
        ChunkUtils.checkSliceArgs(other.size, offset, capacity);
        return resetFromTypedArray(other.data, other.offset + offset, capacity);
    }

    public final <ATTR extends ATTR_BASE> WritableObjectChunk<T, ATTR> resetFromTypedArray(T[] data, int offset, int capacity) {
        ChunkUtils.checkArrayArgs(data.length, offset, capacity);
        this.data = data;
        this.offset = offset;
        this.capacity = capacity;
        this.size = capacity;
        //noinspection unchecked
        return this;
    }

    @Override
    public final void close() {
        MultiChunkPool.forThisThread().getObjectChunkPool().giveResettableWritableObjectChunk(this);
    }
}
