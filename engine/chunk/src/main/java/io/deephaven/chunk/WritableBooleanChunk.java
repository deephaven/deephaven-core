//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit WritableCharChunk and run "./gradlew replicateSourcesAndChunks" to regenerate
//
// @formatter:off
package io.deephaven.chunk;

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
 * {@link WritableChunk} implementation for boolean data.
 */
public class WritableBooleanChunk<ATTR extends Any> extends BooleanChunk<ATTR> implements WritableChunk<ATTR> {

    @SuppressWarnings("rawtypes")
    private static final WritableBooleanChunk[] EMPTY_WRITABLE_BOOLEAN_CHUNK_ARRAY = new WritableBooleanChunk[0];

    static <ATTR extends Any> WritableBooleanChunk<ATTR>[] getEmptyChunkArray() {
        // noinspection unchecked
        return EMPTY_WRITABLE_BOOLEAN_CHUNK_ARRAY;
    }

    public static <ATTR extends Any> WritableBooleanChunk<ATTR> makeWritableChunk(int size) {
        if (POOL_WRITABLE_CHUNKS) {
            return MultiChunkPool.forThisThread().takeWritableBooleanChunk(size);
        }
        return new WritableBooleanChunk<>(makeArray(size), 0, size);
    }

    public static <ATTR extends Any> WritableBooleanChunk<ATTR> makeWritableChunkForPool(int size) {
        return new WritableBooleanChunk<>(makeArray(size), 0, size) {
            @Override
            public void close() {
                MultiChunkPool.forThisThread().giveWritableBooleanChunk(this);
            }
        };
    }

    public static <ATTR extends Any> WritableBooleanChunk<ATTR> writableChunkWrap(boolean[] data) {
        return writableChunkWrap(data, 0, data.length);
    }

    public static <ATTR extends Any> WritableBooleanChunk<ATTR> writableChunkWrap(boolean[] data, int offset, int size) {
        return new WritableBooleanChunk<>(data, offset, size);
    }

    protected WritableBooleanChunk(boolean[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    public final void set(int index, boolean value) {
        data[offset + index] = value;
    }

    public final void add(boolean value) {
        data[offset + size++] = value;
    }

    @Override
    public WritableBooleanChunk<ATTR> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new WritableBooleanChunk<>(data, this.offset + offset, capacity);
    }

    // region array
    /**
     * Get the data array backing this WritableBooleanChunk. The first element of this chunk corresponds to
     * {@code array()[arrayOffset()]}.
     * <p>
     * This WritableBooleanChunk must never be {@link #close() closed} while the array <em>may</em> be in use externally,
     * because it must not be returned to any pool for re-use until that re-use is guaranteed to be exclusive.
     *
     * @return The backing data array
     */
    public final boolean[] array() {
        return data;
    }

    /**
     * Get this WritableBooleanChunk's offset into the backing data array. The first element of this chunk corresponds to
     * {@code array()[arrayOffset()]}.
     *
     * @return The offset into the backing data array
     */
    public final int arrayOffset() {
        return offset;
    }
    // endregion array

    // region FillWithNullValueImpl
    // endregion FillWithNullValueImpl

    // region fillWithBoxedValue
    @Override
    public final void fillWithBoxedValue(int offset, int size, Object value) {
        fillWithValue(offset, size, TypeUtils.unbox((Boolean) value));
    }
    // endregion fillWithBoxedValue

    public final void fillWithValue(final int offset, final int length, final boolean value) {
        final int netOffset = this.offset + offset;
        if (length >= SYSTEM_ARRAYFILL_THRESHOLD) {
            Arrays.fill(data, netOffset, netOffset + length, value);
            return;
        }
        for (int ii = 0; ii < length; ++ii) {
            data[netOffset + ii] = value;
        }
    }

    public final void appendTypedChunk(BooleanChunk<? extends ATTR> src, int srcOffset, int length) {
        copyFromTypedChunk(src, srcOffset, size, length);
        size += length;
    }

    @Override
    public final void copyFromChunk(Chunk<? extends ATTR> src, int srcOffset, int destOffset, int length) {
        final BooleanChunk<? extends ATTR> typedSrc = src.asBooleanChunk();
        copyFromTypedChunk(typedSrc, srcOffset, destOffset, length);
    }

    public final void copyFromTypedChunk(BooleanChunk<? extends ATTR> src, int srcOffset, int destOffset, int length) {
        copyFromTypedArray(src.data, src.offset + srcOffset, destOffset, length);
    }

    @Override
    public final void copyFromArray(Object srcArray, int srcOffset, int destOffset, int length) {
        final boolean[] typedArray = (boolean[]) srcArray;
        copyFromTypedArray(typedArray, srcOffset, destOffset, length);
    }

    public final void copyFromTypedArray(boolean[] src, int srcOffset, int destOffset, int length) {
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
    // endregion sort

    @Override
    public void close() {}

    // region downcast
    public static <ATTR extends Any, ATTR_DERIV extends ATTR> WritableBooleanChunk<ATTR> upcast(
            WritableBooleanChunk<ATTR_DERIV> self) {
        // noinspection unchecked
        return (WritableBooleanChunk<ATTR>) self;
    }
    // endregion downcast
}
