package io.deephaven.chunk;

// @formatter:off

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.util.pools.MultiChunkPool;

import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
// region FillWithNullValueImports
import static io.deephaven.util.QueryConstants.NULL_CHAR;
// endregion FillWithNullValueImports

// region BufferImports
import java.nio.Buffer;
import java.nio.CharBuffer;
// endregion BufferImports

// @formatter:on

/**
 * {@link WritableChunk} implementation for char data.
 */
public class WritableCharChunk<ATTR extends Any> extends CharChunk<ATTR> implements WritableChunk<ATTR> {

    private static final WritableCharChunk[] EMPTY_WRITABLE_CHAR_CHUNK_ARRAY = new WritableCharChunk[0];

    static <ATTR extends Any> WritableCharChunk<ATTR>[] getEmptyChunkArray() {
        //noinspection unchecked
        return EMPTY_WRITABLE_CHAR_CHUNK_ARRAY;
    }

    public static <ATTR extends Any> WritableCharChunk<ATTR> makeWritableChunk(int size) {
        return MultiChunkPool.forThisThread().getCharChunkPool().takeWritableCharChunk(size);
    }

    public static WritableCharChunk makeWritableChunkForPool(int size) {
        return new WritableCharChunk(makeArray(size), 0, size) {
            @Override
            public void close() {
                MultiChunkPool.forThisThread().getCharChunkPool().giveWritableCharChunk(this);
            }
        };
    }

    public static <ATTR extends Any> WritableCharChunk<ATTR> writableChunkWrap(char[] data) {
        return writableChunkWrap(data, 0, data.length);
    }

    public static <ATTR extends Any> WritableCharChunk<ATTR> writableChunkWrap(char[] data, int offset, int size) {
        return new WritableCharChunk<>(data, offset, size);
    }

    WritableCharChunk(char[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    public final void set(int index, char value) {
        data[offset + index] = value;
    }

    public final void add(char value) { data[offset + size++] = value; }

    @Override
    public WritableCharChunk<ATTR> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new WritableCharChunk<>(data, this.offset + offset, capacity);
    }

    // region FillWithNullValueImpl
    @Override
    public final void fillWithNullValue(final int offset, final int length) {
        fillWithValue(offset, length, NULL_CHAR);
    }
    // endregion FillWithNullValueImpl

    // region fillWithBoxedValue
    @Override
    public final void fillWithBoxedValue(int offset, int size, Object value) {
        fillWithValue(offset,size, TypeUtils.unbox((Character) value));
    }
    // endregion fillWithBoxedValue

    public final void fillWithValue(final int offset, final int length, final char value) {
        final int netOffset = this.offset + offset;
        if (length >= SYSTEM_ARRAYFILL_THRESHOLD) {
            Arrays.fill(data, netOffset, netOffset + length, value);
            return;
        }
        for (int ii = 0; ii < length; ++ii) {
            data[netOffset + ii] = value;
        }
    }

    public final void appendTypedChunk(CharChunk<? extends ATTR> src, int srcOffset, int length) {
        copyFromTypedChunk(src, srcOffset, size, length);
        size += length;
    }

    @Override
    public final void copyFromChunk(Chunk<? extends ATTR> src, int srcOffset, int destOffset, int length) {
        final CharChunk<? extends ATTR> typedSrc = src.asCharChunk();
        copyFromTypedChunk(typedSrc, srcOffset, destOffset, length);
    }

    public final void copyFromTypedChunk(CharChunk<? extends ATTR> src, int srcOffset, int destOffset, int length) {
        copyFromTypedArray(src.data, src.offset + srcOffset, destOffset, length);
    }

    @Override
    public final void copyFromArray(Object srcArray, int srcOffset, int destOffset, int length) {
        final char[] typedArray = (char[])srcArray;
        copyFromTypedArray(typedArray, srcOffset, destOffset, length);
    }

    public final void copyFromTypedArray(char[] src, int srcOffset, int destOffset, int length) {
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
        final CharBuffer charSrcBuffer = (CharBuffer) srcBuffer;
        copyFromTypedBuffer(charSrcBuffer, srcOffset, destOffset, length);
    }

    /**
     * <p>Fill a sub-range of this WritableCharChunk with values from a {@link CharBuffer}.
     *
     * <p>See {@link #copyFromBuffer(Buffer, int, int, int)} for general documentation.
     *
     * @param srcBuffer  The source {@link CharBuffer}
     * @param srcOffset  The absolute offset into {@code srcBuffer} to start copying from
     * @param destOffset The offset into this chunk to start copying to
     * @param length     The number of elements to copy
     */
    public final void copyFromTypedBuffer(@NotNull final CharBuffer srcBuffer, final int srcOffset, final int destOffset, final int length) {
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
        if(length <= 1) {
            return;
        }

        int foundLeft = Arrays.binarySearch(data, start, start + length, NULL_CHAR);
        if(foundLeft < 0) {
            return;
        }

        int foundRight = foundLeft;
        while(foundLeft > start && data[foundLeft - 1] == NULL_CHAR) {
            foundLeft--;
        }

        // If the nulls are already the leftmost thing, we are done.
        if(foundLeft > 0) {
            while (foundRight < start + length - 1 && data[foundRight + 1] == NULL_CHAR) {
                foundRight++;
            }

            final int nullCount = foundRight - foundLeft + 1;
            System.arraycopy(data, start, data, start + nullCount, foundLeft - start);
            Arrays.fill(data, start, start + nullCount, NULL_CHAR);
        }
        // endregion SortFixup
    }
    // endregion sort

    @Override
    public void close() {
    }

    // region downcast
    public static <ATTR extends Any, ATTR_DERIV extends ATTR> WritableCharChunk<ATTR> upcast(WritableCharChunk<ATTR_DERIV> self) {
        //noinspection unchecked
        return (WritableCharChunk<ATTR>) self;
    }
    // endregion downcast
}
