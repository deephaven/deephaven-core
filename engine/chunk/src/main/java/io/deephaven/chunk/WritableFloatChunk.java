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
import static io.deephaven.util.QueryConstants.NULL_FLOAT;
// endregion FillWithNullValueImports

// region BufferImports
import java.nio.Buffer;
import java.nio.FloatBuffer;
// endregion BufferImports

import static io.deephaven.chunk.util.pools.ChunkPoolConstants.POOL_WRITABLE_CHUNKS;

/**
 * {@link WritableChunk} implementation for float data.
 */
public class WritableFloatChunk<ATTR extends Any> extends FloatChunk<ATTR> implements WritableChunk<ATTR> {

    @SuppressWarnings("rawtypes")
    private static final WritableFloatChunk[] EMPTY_WRITABLE_FLOAT_CHUNK_ARRAY = new WritableFloatChunk[0];

    static <ATTR extends Any> WritableFloatChunk<ATTR>[] getEmptyChunkArray() {
        // noinspection unchecked
        return EMPTY_WRITABLE_FLOAT_CHUNK_ARRAY;
    }

    public static <ATTR extends Any> WritableFloatChunk<ATTR> makeWritableChunk(int size) {
        if (POOL_WRITABLE_CHUNKS) {
            return MultiChunkPool.forThisThread().takeWritableFloatChunk(size);
        }
        return new WritableFloatChunk<>(makeArray(size), 0, size);
    }

    public static <ATTR extends Any> WritableFloatChunk<ATTR> makeWritableChunkForPool(int size) {
        return new WritableFloatChunk<>(makeArray(size), 0, size) {
            @Override
            public void close() {
                MultiChunkPool.forThisThread().giveWritableFloatChunk(this);
            }
        };
    }

    public static <ATTR extends Any> WritableFloatChunk<ATTR> writableChunkWrap(float[] data) {
        return writableChunkWrap(data, 0, data.length);
    }

    public static <ATTR extends Any> WritableFloatChunk<ATTR> writableChunkWrap(float[] data, int offset, int size) {
        return new WritableFloatChunk<>(data, offset, size);
    }

    protected WritableFloatChunk(float[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    public final void set(int index, float value) {
        data[offset + index] = value;
    }

    public final void add(float value) {
        data[offset + size++] = value;
    }

    @Override
    public WritableFloatChunk<ATTR> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new WritableFloatChunk<>(data, this.offset + offset, capacity);
    }

    // region array
    /**
     * Get the data array backing this WritableFloatChunk. The first element of this chunk corresponds to
     * {@code array()[arrayOffset()]}.
     * <p>
     * This WritableFloatChunk must never be {@link #close() closed} while the array <em>may</em> be in use externally,
     * because it must not be returned to any pool for re-use until that re-use is guaranteed to be exclusive.
     *
     * @return The backing data array
     */
    public final float[] array() {
        return data;
    }

    /**
     * Get this WritableFloatChunk's offset into the backing data array. The first element of this chunk corresponds to
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
        fillWithValue(offset, length, NULL_FLOAT);
    }
    // endregion FillWithNullValueImpl

    // region fillWithBoxedValue
    @Override
    public final void fillWithBoxedValue(int offset, int size, Object value) {
        fillWithValue(offset, size, TypeUtils.unbox((Float) value));
    }
    // endregion fillWithBoxedValue

    public final void fillWithValue(final int offset, final int length, final float value) {
        final int netOffset = this.offset + offset;
        if (length >= SYSTEM_ARRAYFILL_THRESHOLD) {
            Arrays.fill(data, netOffset, netOffset + length, value);
            return;
        }
        for (int ii = 0; ii < length; ++ii) {
            data[netOffset + ii] = value;
        }
    }

    public final void appendTypedChunk(FloatChunk<? extends ATTR> src, int srcOffset, int length) {
        copyFromTypedChunk(src, srcOffset, size, length);
        size += length;
    }

    @Override
    public final void copyFromChunk(Chunk<? extends ATTR> src, int srcOffset, int destOffset, int length) {
        final FloatChunk<? extends ATTR> typedSrc = src.asFloatChunk();
        copyFromTypedChunk(typedSrc, srcOffset, destOffset, length);
    }

    public final void copyFromTypedChunk(FloatChunk<? extends ATTR> src, int srcOffset, int destOffset, int length) {
        copyFromTypedArray(src.data, src.offset + srcOffset, destOffset, length);
    }

    @Override
    public final void copyFromArray(Object srcArray, int srcOffset, int destOffset, int length) {
        final float[] typedArray = (float[]) srcArray;
        copyFromTypedArray(typedArray, srcOffset, destOffset, length);
    }

    public final void copyFromTypedArray(float[] src, int srcOffset, int destOffset, int length) {
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
        final FloatBuffer floatSrcBuffer = (FloatBuffer) srcBuffer;
        copyFromTypedBuffer(floatSrcBuffer, srcOffset, destOffset, length);
    }

    /**
     * <p>
     * Fill a sub-range of this WritableFloatChunk with values from a {@link FloatBuffer}.
     *
     * <p>
     * See {@link #copyFromBuffer(Buffer, int, int, int)} for general documentation.
     *
     * @param srcBuffer The source {@link FloatBuffer}
     * @param srcOffset The absolute offset into {@code srcBuffer} to start copying from
     * @param destOffset The offset into this chunk to start copying to
     * @param length The number of elements to copy
     */
    public final void copyFromTypedBuffer(@NotNull final FloatBuffer srcBuffer, final int srcOffset,
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
    public static <ATTR extends Any, ATTR_DERIV extends ATTR> WritableFloatChunk<ATTR> upcast(
            WritableFloatChunk<ATTR_DERIV> self) {
        // noinspection unchecked
        return (WritableFloatChunk<ATTR>) self;
    }
    // endregion downcast
}
