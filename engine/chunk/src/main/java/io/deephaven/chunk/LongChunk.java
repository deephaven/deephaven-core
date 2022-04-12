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
import java.nio.Buffer;
import java.nio.LongBuffer;
// endregion BufferImports

/**
 * {@link Chunk} implementation for long data.
 */
public class LongChunk<ATTR extends Any> extends ChunkBase<ATTR> {

    private static final LongChunk EMPTY = new LongChunk<>(ArrayTypeUtils.EMPTY_LONG_ARRAY, 0, 0);

    public static <ATTR extends Any> LongChunk<ATTR> getEmptyChunk() {
        //noinspection unchecked
        return EMPTY;
    }

    private static final LongChunk[] EMPTY_LONG_CHUNK_ARRAY = new LongChunk[0];

    static <ATTR extends Any> LongChunk<ATTR>[] getEmptyChunkArray() {
        //noinspection unchecked
        return EMPTY_LONG_CHUNK_ARRAY;
    }

    // region makeArray
    public static long[] makeArray(int capacity) {
        if (capacity == 0) {
            return ArrayTypeUtils.EMPTY_LONG_ARRAY;
        }
        return new long[capacity];
    }
    // endregion makeArray

    public static <ATTR extends Any> LongChunk<ATTR> chunkWrap(long[] data) {
        return chunkWrap(data, 0, data.length);
    }

    public static <ATTR extends Any> LongChunk<ATTR> chunkWrap(long[] data, int offset, int capacity) {
        return new LongChunk<>(data, offset, capacity);
    }

    long[] data;

    protected LongChunk(long[] data, int offset, int capacity) {
        super(data.length, offset, capacity);
        this.data = data;
    }

    public final ChunkType getChunkType() {
        return ChunkType.Long;
    }

    public final long get(int index) {
        return data[offset + index];
    }

    @Override
    public LongChunk<ATTR> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new LongChunk<>(data, this.offset + offset, capacity);
    }

    @Override
    public final void copyToChunk(int srcOffset, WritableChunk<? super ATTR> dest, int destOffset, int length) {
        final WritableLongChunk<? super ATTR> wDest = dest.asWritableLongChunk();
        copyToTypedArray(srcOffset, wDest.data, wDest.offset + destOffset, length);
    }

    @Override
    public final void copyToArray(int srcOffset, Object dest, int destOffset, int length) {
        final long[] realType = (long[])dest;
        copyToTypedArray(srcOffset, realType, destOffset, length);
    }

    public final void copyToTypedArray(int srcOffset, long[] destData, int destOffset, int length) {
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
    public final boolean isAlias(Chunk chunk) {
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
    @Override
    public final void copyToBuffer(final int srcOffset, @NotNull final Buffer destBuffer, final int destOffset, final int length) {
        final LongBuffer longDestBuffer = (LongBuffer) destBuffer;
        copyToTypedBuffer(srcOffset, longDestBuffer, destOffset, length);
    }

    /**
     * <p>Copy a sub-range of this LongChunk to a {@link LongBuffer}.
     *
     * <p>See {@link #copyToBuffer(int, Buffer, int, int)} for general documentation.
     *
     * @param srcOffset  The offset into this chunk to start copying from
     * @param destBuffer The destination {@link LongBuffer}
     * @param destOffset The absolute offset into {@code destBuffer} to start copying to
     * @param length     The number of elements to copy
     */
    public final void copyToTypedBuffer(final int srcOffset, @NotNull final LongBuffer destBuffer, final int destOffset, final int length) {
        if (destBuffer.hasArray()) {
            copyToTypedArray(srcOffset, destBuffer.array(), destBuffer.arrayOffset() + destOffset, length);
            return;
        }
        final int initialPosition = destBuffer.position();
        destBuffer.position(destOffset);
        destBuffer.put(data, offset + srcOffset, length);
        destBuffer.position(initialPosition);
    }
    // endregion CopyToBuffer

    // region downcast
    public static <ATTR extends Any, ATTR_DERIV extends ATTR> WritableLongChunk<ATTR_DERIV> downcast(WritableLongChunk<ATTR> self) {
        //noinspection unchecked
        return (WritableLongChunk<ATTR_DERIV>) self;
    }
    // endregion downcast
}
