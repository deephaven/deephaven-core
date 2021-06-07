/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ResettableCharChunk and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.sources.chunk;
import io.deephaven.db.tables.utils.ArrayUtils;
import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.util.pools.MultiChunkPool;
import io.deephaven.db.v2.utils.ChunkUtils;

/**
 * {@link ResettableReadOnlyChunk} implementation for Object data.
 */
public final class ResettableObjectChunk<T, ATTR_UPPER extends Any> extends ObjectChunk implements ResettableReadOnlyChunk<ATTR_UPPER> {

    public static <T, ATTR_BASE extends Any> ResettableObjectChunk<T, ATTR_BASE> makeResettableChunk() {
        return MultiChunkPool.forThisThread().getObjectChunkPool().takeResettableObjectChunk();
    }

    public static <T, ATTR_BASE extends Any> ResettableObjectChunk<T, ATTR_BASE> makeResettableChunkForPool() {
        return new ResettableObjectChunk<>();
    }

    private ResettableObjectChunk(T[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    private ResettableObjectChunk() {
        //noinspection unchecked
        this((T[])ArrayUtils.EMPTY_OBJECT_ARRAY, 0, 0);
    }

    @Override
    public final ResettableObjectChunk slice(int offset, int capacity) {
        ChunkUtils.checkSliceArgs(size, offset, capacity);
        return new ResettableObjectChunk(data, this.offset + offset, capacity);
    }

    @Override
    public final <ATTR extends ATTR_UPPER> ObjectChunk<T, ATTR> resetFromChunk(Chunk<? extends ATTR> other, int offset, int capacity) {
        return resetFromTypedChunk(other.asObjectChunk(), offset, capacity);
    }

    @Override
    public final <ATTR extends ATTR_UPPER> ObjectChunk<T, ATTR> resetFromArray(Object array, int offset, int capacity) {
        //noinspection unchecked
        final T[] typedArray = (T[])array;
        return resetFromTypedArray(typedArray, offset, capacity);
    }

    @Override
    public final <ATTR extends ATTR_UPPER> ObjectChunk<T, ATTR> resetFromArray(Object array) {
        //noinspection unchecked
        final T[] typedArray = (T[])array;
        return resetFromTypedArray(typedArray, 0, typedArray.length);
    }

    @Override
    public final <ATTR extends ATTR_UPPER> ObjectChunk<T, ATTR> clear() {
        return resetFromArray(ArrayUtils.EMPTY_OBJECT_ARRAY, 0, 0);
    }

    public final <ATTR extends ATTR_UPPER> ObjectChunk<T, ATTR> resetFromTypedChunk(ObjectChunk<T, ? extends ATTR> other, int offset, int capacity) {
        ChunkUtils.checkSliceArgs(other.size, offset, capacity);
        return resetFromTypedArray(other.data, other.offset + offset, capacity);
    }

    public final <ATTR extends ATTR_UPPER> ObjectChunk<T, ATTR> resetFromTypedArray(T[] data, int offset, int capacity) {
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
        MultiChunkPool.forThisThread().getObjectChunkPool().giveResettableObjectChunk(this);
    }
}
