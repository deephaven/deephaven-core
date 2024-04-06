//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit WritableCharChunk and run "./gradlew replicateSourcesAndChunks" to regenerate
//
// @formatter:off
package io.deephaven.chunk;

import io.deephaven.util.compare.ObjectComparisons;
import java.util.Comparator;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.util.pools.MultiChunkPool;

import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
// region FillWithNullValueImports
// endregion FillWithNullValueImports

// region BufferImports
// endregion BufferImports

import static io.deephaven.chunk.util.pools.ChunkPoolConstants.POOL_WRITABLE_CHUNKS;

/**
 * {@link WritableChunk} implementation for Object data.
 */
public class WritableObjectChunk<T, ATTR extends Any> extends ObjectChunk<T, ATTR> implements WritableChunk<ATTR> {

    @SuppressWarnings("rawtypes")
    private static final WritableObjectChunk[] EMPTY_WRITABLE_OBJECT_CHUNK_ARRAY = new WritableObjectChunk[0];

    static <T, ATTR extends Any> WritableObjectChunk<T, ATTR>[] getEmptyChunkArray() {
        // noinspection unchecked
        return EMPTY_WRITABLE_OBJECT_CHUNK_ARRAY;
    }

    public static <T, ATTR extends Any> WritableObjectChunk<T, ATTR> makeWritableChunk(int size) {
        if (POOL_WRITABLE_CHUNKS) {
            return MultiChunkPool.forThisThread().takeWritableObjectChunk(size);
        }
        return new WritableObjectChunk<>(makeArray(size), 0, size);
    }

    public static <T, ATTR extends Any> WritableObjectChunk<T, ATTR> makeWritableChunkForPool(int size) {
        return new WritableObjectChunk<>(makeArray(size), 0, size) {
            @Override
            public void close() {
                MultiChunkPool.forThisThread().giveWritableObjectChunk(this);
            }
        };
    }

    public static <T, ATTR extends Any> WritableObjectChunk<T, ATTR> writableChunkWrap(T[] data) {
        return writableChunkWrap(data, 0, data.length);
    }

    public static <T, ATTR extends Any> WritableObjectChunk<T, ATTR> writableChunkWrap(T[] data, int offset, int size) {
        return new WritableObjectChunk<>(data, offset, size);
    }

    protected WritableObjectChunk(T[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    public final void set(int index, T value) {
        data[offset + index] = value;
    }

    public final void add(T value) {
        data[offset + size++] = value;
    }

    @Override
    public WritableObjectChunk<T, ATTR> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new WritableObjectChunk<>(data, this.offset + offset, capacity);
    }

    // region array
    /**
     * Get the data array backing this WritableObjectChunk. The first element of this chunk corresponds to
     * {@code array()[arrayOffset()]}.
     * <p>
     * This WritableObjectChunk must never be {@link #close() closed} while the array <em>may</em> be in use externally,
     * because it must not be returned to any pool for re-use until that re-use is guaranteed to be exclusive.
     *
     * @return The backing data array
     */
    public final T[] array() {
        return data;
    }

    /**
     * Get this WritableObjectChunk's offset into the backing data array. The first element of this chunk corresponds to
     * {@code array()[arrayOffset()]}.
     *
     * @return The offset into the backing data array
     */
    public final int arrayOffset() {
        return offset;
    }
    // endregion array

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
        final T[] typedArray = (T[]) srcArray;
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
            // noinspection ManualArrayCopy
            for (int ii = 0; ii < length; ++ii) {
                data[netDestOffset + ii] = src[srcOffset + ii];
            }
            return;
        }
        // noinspection ManualArrayCopy
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
    public void close() {}

    // region downcast
    public <T_DERIV extends T> WritableObjectChunk<T_DERIV, ATTR> asTypedWritableObjectChunk() {
        //noinspection unchecked
        return (WritableObjectChunk<T_DERIV, ATTR>) this;
    }

    public static <T, ATTR extends Any, ATTR_DERIV extends ATTR> WritableObjectChunk<T, ATTR> upcast(
            WritableObjectChunk<T, ATTR_DERIV> self) {
        // noinspection unchecked
        return (WritableObjectChunk<T, ATTR>) self;
    }
    // endregion downcast
}
