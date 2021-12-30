/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit WritableCharChunk and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk;

import io.deephaven.util.compare.ObjectComparisons;
import java.util.Comparator;

// @formatter:off

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.util.pools.MultiChunkPool;

import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
// region FillWithNullValueImports
// endregion FillWithNullValueImports

// region BufferImports
// endregion BufferImports

// @formatter:on

/**
 * {@link WritableChunk} implementation for Object data.
 */
public class WritableObjectChunk<T, ATTR extends Any> extends ObjectChunk<T, ATTR> implements WritableChunk<ATTR> {

    private static final WritableObjectChunk[] EMPTY_WRITABLE_OBJECT_CHUNK_ARRAY = new WritableObjectChunk[0];

    static <T, ATTR extends Any> WritableObjectChunk<T, ATTR>[] getEmptyChunkArray() {
        //noinspection unchecked
        return EMPTY_WRITABLE_OBJECT_CHUNK_ARRAY;
    }

    public static <T, ATTR extends Any> WritableObjectChunk<T, ATTR> makeWritableChunk(int size) {
        return MultiChunkPool.forThisThread().getObjectChunkPool().takeWritableObjectChunk(size);
    }

    public static WritableObjectChunk makeWritableChunkForPool(int size) {
        return new WritableObjectChunk(makeArray(size), 0, size) {
            @Override
            public void close() {
                MultiChunkPool.forThisThread().getObjectChunkPool().giveWritableObjectChunk(this);
            }
        };
    }

    public static <T, ATTR extends Any> WritableObjectChunk<T, ATTR> writableChunkWrap(T[] data) {
        return writableChunkWrap(data, 0, data.length);
    }

    public static <T, ATTR extends Any> WritableObjectChunk<T, ATTR> writableChunkWrap(T[] data, int offset, int size) {
        return new WritableObjectChunk<>(data, offset, size);
    }

    WritableObjectChunk(T[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    public final void set(int index, T value) {
        data[offset + index] = value;
    }

    public final void add(T value) { data[offset + size++] = value; }

    @Override
    public WritableObjectChunk<T, ATTR> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new WritableObjectChunk<>(data, this.offset + offset, capacity);
    }

    // region FillWithNullValueImpl
    @Override
    public final void fillWithNullValue(final int offset, final int length) {
        fillWithValue(offset, length, null);
    }
    // endregion FillWithNullValueImpl

    // region fillWithBoxedValue
    @Override
    public final void fillWithBoxedValue(int offset, int size, Object value) {
        fillWithValue(offset,size, (T)value);
    }
    // endregion fillWithBoxedValue

    public final void fillWithValue(final int offset, final int length, final T value) {
        final int netOffset = this.offset + offset;
        if (length >= SYSTEM_ARRAYFILL_THRESHOLD) {
            Arrays.fill(data, netOffset, netOffset + length, value);
            return;
        }
        for (int ii = 0; ii < length; ++ii) {
            data[netOffset + ii] = value;
        }
    }

    public final void appendTypedChunk(ObjectChunk<T, ? extends ATTR> src, int srcOffset, int length) {
        copyFromTypedChunk(src, srcOffset, size, length);
        size += length;
    }

    @Override
    public final void copyFromChunk(Chunk<? extends ATTR> src, int srcOffset, int destOffset, int length) {
        final ObjectChunk<T, ? extends ATTR> typedSrc = src.asObjectChunk();
        copyFromTypedChunk(typedSrc, srcOffset, destOffset, length);
    }

    public final void copyFromTypedChunk(ObjectChunk<T, ? extends ATTR> src, int srcOffset, int destOffset, int length) {
        copyFromTypedArray(src.data, src.offset + srcOffset, destOffset, length);
    }

    @Override
    public final void copyFromArray(Object srcArray, int srcOffset, int destOffset, int length) {
        //noinspection unchecked
        final T[] typedArray = (T[])srcArray;
        copyFromTypedArray(typedArray, srcOffset, destOffset, length);
    }

    public final void copyFromTypedArray(T[] src, int srcOffset, int destOffset, int length) {
        final int netDestOffset = offset + destOffset;
        if (length >= SYSTEM_ARRAYCOPY_THRESHOLD) {
            // I wonder if this is wasteful because we already know the concrete type of src and data.
            System.arraycopy(src, srcOffset, data, netDestOffset, length);
            return;
        }
        if (ChunkHelpers.canCopyForward(src, srcOffset, data, destOffset, length)) {
            //noinspection ManualArrayCopy
            for (int ii = 0; ii < length; ++ii) {
                data[netDestOffset + ii] = src[srcOffset + ii];
            }
            return;
        }
        //noinspection ManualArrayCopy
        for (int ii = length - 1; ii >= 0; --ii) {
            data[netDestOffset + ii] = src[srcOffset + ii];
        }
    }

    // region CopyFromBuffer
    // endregion CopyFromBuffer

    @Override
    public final void sort() {
        sort(0, size);
    }

    // region sort
    private static final Comparator<Comparable<Object>> COMPARATOR = Comparator.nullsFirst(Comparator.naturalOrder());

    @Override
    public final void sort(int start, int length) {
        //noinspection unchecked
        Arrays.sort(data, offset + start, offset + start + length, (Comparator<? super T>) COMPARATOR);
    }
    // endregion sort

    @Override
    public void close() {
    }

    // region downcast
    public <T_DERIV extends T> WritableObjectChunk<T_DERIV, ATTR> asTypedWritableObjectChunk() {
        //noinspection unchecked
        return (WritableObjectChunk<T_DERIV, ATTR>) this;
    }

    public static <T, ATTR extends Any, ATTR_DERIV extends ATTR> WritableObjectChunk<T, ATTR> upcast(WritableObjectChunk<T, ATTR_DERIV> self) {
        //noinspection unchecked
        return (WritableObjectChunk<T, ATTR>) self;
    }
    // endregion downcast
}
