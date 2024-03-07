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
import static io.deephaven.util.QueryConstants.NULL_LONG;
// endregion FillWithNullValueImports

// region BufferImports
import java.nio.Buffer;
import java.nio.LongBuffer;
// endregion BufferImports

import static io.deephaven.chunk.util.pools.ChunkPoolConstants.POOL_WRITABLE_CHUNKS;

/**
 * {@link WritableChunk} implementation for long data.
 */
public class WritableLongChunk<ATTR extends Any> extends LongChunk<ATTR> implements WritableChunk<ATTR> {

    @SuppressWarnings("rawtypes")
    private static final WritableLongChunk[] EMPTY_WRITABLE_LONG_CHUNK_ARRAY = new WritableLongChunk[0];

    static <ATTR extends Any> WritableLongChunk<ATTR>[] getEmptyChunkArray() {
        // noinspection unchecked
        return EMPTY_WRITABLE_LONG_CHUNK_ARRAY;
    }

    public static <ATTR extends Any> WritableLongChunk<ATTR> makeWritableChunk(int size) {
        if (POOL_WRITABLE_CHUNKS) {
            return MultiChunkPool.forThisThread().takeWritableLongChunk(size);
        }
        return new WritableLongChunk<>(makeArray(size), 0, size);
    }

    public static <ATTR extends Any> WritableLongChunk<ATTR> makeWritableChunkForPool(int size) {
        return new WritableLongChunk<>(makeArray(size), 0, size) {
            @Override
            public void close() {
                MultiChunkPool.forThisThread().giveWritableLongChunk(this);
            }
        };
    }

    public static <ATTR extends Any> WritableLongChunk<ATTR> writableChunkWrap(long[] data) {
        return writableChunkWrap(data, 0, data.length);
    }

    public static <ATTR extends Any> WritableLongChunk<ATTR> writableChunkWrap(long[] data, int offset, int size) {
        return new WritableLongChunk<>(data, offset, size);
    }

    protected WritableLongChunk(long[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    public final void set(int index, long value) {
        data[offset + index] = value;
    }

    public final void add(long value) {
        data[offset + size++] = value;
    }

    @Override
    public WritableLongChunk<ATTR> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new WritableLongChunk<>(data, this.offset + offset, capacity);
    }

    // region array
    /**
     * Get the data array backing this WritableLongChunk. The first element of this chunk corresponds to
     * {@code array()[arrayOffset()]}.
     * <p>
     * This WritableLongChunk must never be {@link #close() closed} while the array <em>may</em> be in use externally,
     * because it must not be returned to any pool for re-use until that re-use is guaranteed to be exclusive.
     *
     * @return The backing data array
     */
    public final long[] array() {
        return data;
    }

    /**
     * Get this WritableLongChunk's offset into the backing data array. The first element of this chunk corresponds to
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
        fillWithValue(offset, length, NULL_LONG);
    }
    // endregion FillWithNullValueImpl

    // region fillWithBoxedValue
    @Override
    public final void fillWithBoxedValue(int offset, int size, Object value) {
        fillWithValue(offset, size, TypeUtils.unbox((Long) value));
    }
    // endregion fillWithBoxedValue

    public final void fillWithValue(final int offset, final int length, final long value) {
        final int netOffset = this.offset + offset;
        if (length >= SYSTEM_ARRAYFILL_THRESHOLD) {
            Arrays.fill(data, netOffset, netOffset + length, value);
            return;
        }
        for (int ii = 0; ii < length; ++ii) {
            data[netOffset + ii] = value;
        }
    }

    public final void appendTypedChunk(LongChunk<? extends ATTR> src, int srcOffset, int length) {
        copyFromTypedChunk(src, srcOffset, size, length);
        size += length;
    }

    @Override
    public final void copyFromChunk(Chunk<? extends ATTR> src, int srcOffset, int destOffset, int length) {
        final LongChunk<? extends ATTR> typedSrc = src.asLongChunk();
        copyFromTypedChunk(typedSrc, srcOffset, destOffset, length);
    }

    public final void copyFromTypedChunk(LongChunk<? extends ATTR> src, int srcOffset, int destOffset, int length) {
        copyFromTypedArray(src.data, src.offset + srcOffset, destOffset, length);
    }

    @Override
    public final void copyFromArray(Object srcArray, int srcOffset, int destOffset, int length) {
        final long[] typedArray = (long[]) srcArray;
        copyFromTypedArray(typedArray, srcOffset, destOffset, length);
    }

    public final void copyFromTypedArray(long[] src, int srcOffset, int destOffset, int length) {
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
    @Override
    public final void copyFromBuffer(@NotNull final Buffer srcBuffer, final int srcOffset, final int destOffset,
            final int length) {
        final LongBuffer longSrcBuffer = (LongBuffer) srcBuffer;
        copyFromTypedBuffer(longSrcBuffer, srcOffset, destOffset, length);
    }

    /**
     * <p>
     * Fill a sub-range of this WritableLongChunk with values from a {@link LongBuffer}.
     *
     * <p>
     * See {@link #copyFromBuffer(Buffer, int, int, int)} for general documentation.
     *
     * @param srcBuffer The source {@link LongBuffer}
     * @param srcOffset The absolute offset into {@code srcBuffer} to start copying from
     * @param destOffset The offset into this chunk to start copying to
     * @param length The number of elements to copy
     */
    public final void copyFromTypedBuffer(@NotNull final LongBuffer srcBuffer, final int srcOffset,
            final int destOffset, final int length) {
        if (srcBuffer.hasArray()) {
            copyFromTypedArray(srcBuffer.array(), srcBuffer.arrayOffset() + srcOffset, destOffset, length);
        } else {
            final int initialPosition = srcBuffer.position();
            srcBuffer.position(srcOffset);
            srcBuffer.get(data, offset + destOffset, length);
            srcBuffer.position(initialPosition);
        }
    }
    // endregion CopyFromBuffer

    @Override
    public final void sort() {
        sort(0, size);
    }

    // region sort
    @Override
    public final void sort(int start, int length) {
        Arrays.sort(data, offset + start, offset + start + length);

        // region SortFixup
        // endregion SortFixup
    }
    // endregion sort

    @Override
    public void close() {}

    // region downcast
    public static <ATTR extends Any, ATTR_DERIV extends ATTR> WritableLongChunk<ATTR> upcast(
            WritableLongChunk<ATTR_DERIV> self) {
        // noinspection unchecked
        return (WritableLongChunk<ATTR>) self;
    }
    // endregion downcast
}
