/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit WritableCharChunk and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk;

// @formatter:off

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.util.pools.MultiChunkPool;

import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
// region FillWithNullValueImports
import static io.deephaven.util.QueryConstants.NULL_INT;
// endregion FillWithNullValueImports

// region BufferImports
import java.nio.Buffer;
import java.nio.IntBuffer;
// endregion BufferImports

// @formatter:on

/**
 * {@link WritableChunk} implementation for int data.
 */
public class WritableIntChunk<ATTR extends Any> extends IntChunk<ATTR> implements WritableChunk<ATTR> {

    private static final WritableIntChunk[] EMPTY_WRITABLE_INT_CHUNK_ARRAY = new WritableIntChunk[0];

    static <ATTR extends Any> WritableIntChunk<ATTR>[] getEmptyChunkArray() {
        //noinspection unchecked
        return EMPTY_WRITABLE_INT_CHUNK_ARRAY;
    }

    public static <ATTR extends Any> WritableIntChunk<ATTR> makeWritableChunk(int size) {
        return MultiChunkPool.forThisThread().getIntChunkPool().takeWritableIntChunk(size);
    }

    public static WritableIntChunk makeWritableChunkForPool(int size) {
        return new WritableIntChunk(makeArray(size), 0, size) {
            @Override
            public void close() {
                MultiChunkPool.forThisThread().getIntChunkPool().giveWritableIntChunk(this);
            }
        };
    }

    public static <ATTR extends Any> WritableIntChunk<ATTR> writableChunkWrap(int[] data) {
        return writableChunkWrap(data, 0, data.length);
    }

    public static <ATTR extends Any> WritableIntChunk<ATTR> writableChunkWrap(int[] data, int offset, int size) {
        return new WritableIntChunk<>(data, offset, size);
    }

    WritableIntChunk(int[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    public final void set(int index, int value) {
        data[offset + index] = value;
    }

    public final void add(int value) { data[offset + size++] = value; }

    @Override
    public WritableIntChunk<ATTR> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new WritableIntChunk<>(data, this.offset + offset, capacity);
    }

    // region FillWithNullValueImpl
    @Override
    public final void fillWithNullValue(final int offset, final int length) {
        fillWithValue(offset, length, NULL_INT);
    }
    // endregion FillWithNullValueImpl

    // region fillWithBoxedValue
    @Override
    public final void fillWithBoxedValue(int offset, int size, Object value) {
        fillWithValue(offset,size, TypeUtils.unbox((Integer) value));
    }
    // endregion fillWithBoxedValue

    public final void fillWithValue(final int offset, final int length, final int value) {
        final int netOffset = this.offset + offset;
        if (length >= SYSTEM_ARRAYFILL_THRESHOLD) {
            Arrays.fill(data, netOffset, netOffset + length, value);
            return;
        }
        for (int ii = 0; ii < length; ++ii) {
            data[netOffset + ii] = value;
        }
    }

    public final void appendTypedChunk(IntChunk<? extends ATTR> src, int srcOffset, int length) {
        copyFromTypedChunk(src, srcOffset, size, length);
        size += length;
    }

    @Override
    public final void copyFromChunk(Chunk<? extends ATTR> src, int srcOffset, int destOffset, int length) {
        final IntChunk<? extends ATTR> typedSrc = src.asIntChunk();
        copyFromTypedChunk(typedSrc, srcOffset, destOffset, length);
    }

    public final void copyFromTypedChunk(IntChunk<? extends ATTR> src, int srcOffset, int destOffset, int length) {
        copyFromTypedArray(src.data, src.offset + srcOffset, destOffset, length);
    }

    @Override
    public final void copyFromArray(Object srcArray, int srcOffset, int destOffset, int length) {
        final int[] typedArray = (int[])srcArray;
        copyFromTypedArray(typedArray, srcOffset, destOffset, length);
    }

    public final void copyFromTypedArray(int[] src, int srcOffset, int destOffset, int length) {
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
    @Override
    public final void copyFromBuffer(@NotNull final Buffer srcBuffer, final int srcOffset, final int destOffset, final int length) {
        final IntBuffer intSrcBuffer = (IntBuffer) srcBuffer;
        copyFromTypedBuffer(intSrcBuffer, srcOffset, destOffset, length);
    }

    /**
     * <p>Fill a sub-range of this WritableIntChunk with values from a {@link IntBuffer}.
     *
     * <p>See {@link #copyFromBuffer(Buffer, int, int, int)} for general documentation.
     *
     * @param srcBuffer  The source {@link IntBuffer}
     * @param srcOffset  The absolute offset into {@code srcBuffer} to start copying from
     * @param destOffset The offset into this chunk to start copying to
     * @param length     The number of elements to copy
     */
    public final void copyFromTypedBuffer(@NotNull final IntBuffer srcBuffer, final int srcOffset, final int destOffset, final int length) {
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
    public void close() {
    }

    // region downcast
    public static <ATTR extends Any, ATTR_DERIV extends ATTR> WritableIntChunk<ATTR> upcast(WritableIntChunk<ATTR_DERIV> self) {
        //noinspection unchecked
        return (WritableIntChunk<ATTR>) self;
    }
    // endregion downcast
}
