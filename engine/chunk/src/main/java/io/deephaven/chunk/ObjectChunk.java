/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunk and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk;

import io.deephaven.util.type.ArrayTypeUtils;
import io.deephaven.chunk.attributes.Any;

import org.jetbrains.annotations.NotNull;

// region ApplyDecoderImports
// endregion ApplyDecoderImports

// region BufferImports
// endregion BufferImports

// region BinarySearchImports
import java.util.Arrays;
// endregion BinarySearchImports

/**
 * {@link Chunk} implementation for Object data.
 */
public class ObjectChunk<T, ATTR extends Any> extends ChunkBase<ATTR> {

    @SuppressWarnings("rawtypes")
    private static final ObjectChunk EMPTY = new ObjectChunk<>(ArrayTypeUtils.EMPTY_OBJECT_ARRAY, 0, 0);

    public static <T, ATTR extends Any> ObjectChunk<T, ATTR> getEmptyChunk() {
        //noinspection unchecked
        return EMPTY;
    }

    @SuppressWarnings("rawtypes")
    private static final ObjectChunk[] EMPTY_OBJECT_CHUNK_ARRAY = new ObjectChunk[0];

    static <T, ATTR extends Any> ObjectChunk<T, ATTR>[] getEmptyChunkArray() {
        //noinspection unchecked
        return EMPTY_OBJECT_CHUNK_ARRAY;
    }

    // region makeArray
    public static <T> T[] makeArray(int capacity) {
        if (capacity == 0) {
            //noinspection unchecked
            return (T[]) ArrayTypeUtils.EMPTY_OBJECT_ARRAY;
        }
        //noinspection unchecked
        return (T[])new Object[capacity];
    }
    // endregion makeArray

    public static <T, ATTR extends Any> ObjectChunk<T, ATTR> chunkWrap(T[] data) {
        return chunkWrap(data, 0, data.length);
    }

    public static <T, ATTR extends Any> ObjectChunk<T, ATTR> chunkWrap(T[] data, int offset, int capacity) {
        return new ObjectChunk<>(data, offset, capacity);
    }

    T[] data;

    protected ObjectChunk(T[] data, int offset, int capacity) {
        super(data.length, offset, capacity);
        this.data = data;
    }

    public final ChunkType getChunkType() {
        return ChunkType.Object;
    }

    public final T get(int index) {
        return data[offset + index];
    }

    @Override
    public ObjectChunk<T, ATTR> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new ObjectChunk<>(data, this.offset + offset, capacity);
    }

    @Override
    public final void copyToChunk(int srcOffset, WritableChunk<? super ATTR> dest, int destOffset, int length) {
        final WritableObjectChunk<T, ? super ATTR> wDest = dest.asWritableObjectChunk();
        copyToTypedArray(srcOffset, wDest.data, wDest.offset + destOffset, length);
    }

    @Override
    public final void copyToArray(int srcOffset, Object dest, int destOffset, int length) {
        final T[] realType = (T[])dest;
        copyToTypedArray(srcOffset, realType, destOffset, length);
    }

    public final void copyToTypedArray(int srcOffset, T[] destData, int destOffset, int length) {
        final int sStart = offset + srcOffset;
        if (length >= SYSTEM_ARRAYCOPY_THRESHOLD) {
            System.arraycopy(data, sStart, destData, destOffset, length);
            return;
        }
        if (ChunkHelpers.canCopyForward(data, sStart, destData, destOffset, length)) {
            //noinspection ManualArrayCopy
            for (int ii = 0; ii < length; ++ii) {
                destData[destOffset + ii] = data[sStart + ii];
            }
            return;
        }
        //noinspection ManualArrayCopy
        for (int ii = length - 1; ii >= 0; --ii) {
            destData[destOffset + ii] = data[sStart + ii];
        }
    }

    @Override
    public final boolean isAlias(Object array) {
        return data == array;
    }

    @Override
    public final boolean isAlias(Chunk<?> chunk) {
        return chunk.isAlias(data);
    }

    @Override
    public final <V extends Visitor<ATTR>> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    // region ApplyDecoder
    // endregion ApplyDecoder

    // region CopyToBuffer
    // endregion CopyToBuffer

    // region downcast
    public <T_DERIV extends T> ObjectChunk<T_DERIV, ATTR> asTypedObjectChunk() {
        //noinspection unchecked
        return (ObjectChunk<T_DERIV, ATTR>) this;
    }

    public static <T, ATTR extends Any, ATTR_DERIV extends ATTR> ObjectChunk<T, ATTR_DERIV> downcast(ObjectChunk<T, ATTR> self) {
        //noinspection unchecked
        return (ObjectChunk<T, ATTR_DERIV>) self;
    }
    // endregion downcast

    // region BinarySearch
    /**
     * Search for {@code key} in this chunk in the index range [0, {@link #size() size}) using Java's primitive
     * ordering. This chunk must be sorted as by {@link WritableObjectChunk#sort()} prior to this call.
     * <p>
     * This method does <em>not</em> compare {@code null} or {@code NaN} values according to Deephaven ordering rules.
     *
     * @param key The key to search for
     * @return The index of the key in this chunk, or else {@code (-(insertion point - 1)} as defined by
     *         {@link Arrays#binarySearch(T[], Object)}
     */
    public final int binarySearch(final Object key) {
        return Arrays.binarySearch(data, offset, offset + size, key);
    }

    /**
     * Search for {@code key} in this chunk in the index range {@code [fromIndexInclusive, toIndexExclusive)} using
     * Java's primitive ordering. This chunk must be sorted over the search index range as by
     * {@link WritableObjectChunk#sort(int, int)} prior to this call.
     * <p>
     * This method does <em>not</em> compare {@code null} or {@code NaN} values according to Deephaven ordering rules.
     *
     * @param fromIndexInclusive The first index to be searched
     * @param toIndexExclusive The index after the last index to be searched
     * @param key The key to search for
     * @return The index of the key in this chunk, or else {@code (-(insertion point - 1)} as defined by
     *         {@link Arrays#binarySearch(T[], int, int, Object)}
     */
    public final int binarySearch(final int fromIndexInclusive, final int toIndexExclusive, final Object key) {
        return Arrays.binarySearch(data, offset + fromIndexInclusive, offset + toIndexExclusive, key);
    }
    // endregion BinarySearch
}
