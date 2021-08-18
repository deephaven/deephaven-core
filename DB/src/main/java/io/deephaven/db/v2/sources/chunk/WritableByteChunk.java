/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit WritableCharChunk and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.sources.chunk;

// @formatter:off

import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.util.chunkfillers.ByteChunkFiller;
import io.deephaven.db.v2.sources.chunk.util.chunkfillers.ChunkFiller;
import io.deephaven.db.v2.sources.chunk.util.pools.MultiChunkPool;
import io.deephaven.db.v2.utils.ChunkUtils;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
// region FillWithNullValueImports
import static io.deephaven.util.QueryConstants.NULL_BYTE;
// endregion FillWithNullValueImports

// region BufferImports
import java.nio.Buffer;
import java.nio.ByteBuffer;
// endregion BufferImports

// @formatter:on

/**
 * {@link WritableChunk} implementation for byte data.
 */
public class WritableByteChunk<ATTR extends Any> extends ByteChunk<ATTR> implements WritableChunk<ATTR> {

    private static final WritableByteChunk[] EMPTY_WRITABLE_BYTE_CHUNK_ARRAY = new WritableByteChunk[0];

    static <ATTR extends Any> WritableByteChunk<ATTR>[] getEmptyChunkArray() {
        //noinspection unchecked
        return EMPTY_WRITABLE_BYTE_CHUNK_ARRAY;
    }

    public static <ATTR extends Any> WritableByteChunk<ATTR> makeWritableChunk(int size) {
        return MultiChunkPool.forThisThread().getByteChunkPool().takeWritableByteChunk(size);
    }

    public static WritableByteChunk makeWritableChunkForPool(int size) {
        return new WritableByteChunk(makeArray(size), 0, size) {
            @Override
            public void close() {
                MultiChunkPool.forThisThread().getByteChunkPool().giveWritableByteChunk(this);
            }
        };
    }

    public static <ATTR extends Any> WritableByteChunk<ATTR> writableChunkWrap(byte[] data) {
        return writableChunkWrap(data, 0, data.length);
    }

    public static <ATTR extends Any> WritableByteChunk<ATTR> writableChunkWrap(byte[] data, int offset, int size) {
        return new WritableByteChunk<>(data, offset, size);
    }

    WritableByteChunk(byte[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    public final void set(int index, byte value) {
        data[offset + index] = value;
    }

    public final void add(byte value) { data[offset + size++] = value; }

    @Override
    public WritableByteChunk<ATTR> slice(int offset, int capacity) {
        ChunkUtils.checkSliceArgs(size, offset, capacity);
        return new WritableByteChunk<>(data, this.offset + offset, capacity);
    }

    // region FillWithNullValueImpl
    @Override
    public final void fillWithNullValue(final int offset, final int length) {
        fillWithValue(offset, length, NULL_BYTE);
    }
    // endregion FillWithNullValueImpl

    // region fillWithBoxedValue
    @Override
    public final void fillWithBoxedValue(int offset, int size, Object value) {
        fillWithValue(offset,size, TypeUtils.unbox((Byte) value));
    }
    // endregion fillWithBoxedValue

    public final void fillWithValue(final int offset, final int length, final byte value) {
        final int netOffset = this.offset + offset;
        if (length >= SYSTEM_ARRAYFILL_THRESHOLD) {
            Arrays.fill(data, netOffset, netOffset + length, value);
            return;
        }
        for (int ii = 0; ii < length; ++ii) {
            data[netOffset + ii] = value;
        }
    }

    public final void appendTypedChunk(ByteChunk<? extends ATTR> src, int srcOffset, int length) {
        copyFromTypedChunk(src, srcOffset, size, length);
        size += length;
    }

    @Override
    public final void copyFromChunk(Chunk<? extends ATTR> src, int srcOffset, int destOffset, int length) {
        final ByteChunk<? extends ATTR> typedSrc = src.asByteChunk();
        copyFromTypedChunk(typedSrc, srcOffset, destOffset, length);
    }

    public final void copyFromTypedChunk(ByteChunk<? extends ATTR> src, int srcOffset, int destOffset, int length) {
        copyFromTypedArray(src.data, src.offset + srcOffset, destOffset, length);
    }

    @Override
    public final void copyFromArray(Object srcArray, int srcOffset, int destOffset, int length) {
        final byte[] typedArray = (byte[])srcArray;
        copyFromTypedArray(typedArray, srcOffset, destOffset, length);
    }

    public final void copyFromTypedArray(byte[] src, int srcOffset, int destOffset, int length) {
        final int netDestOffset = offset + destOffset;
        if (length >= SYSTEM_ARRAYCOPY_THRESHOLD) {
            // I wonder if this is wasteful because we already know the concrete type of src and data.
            System.arraycopy(src, srcOffset, data, netDestOffset, length);
            return;
        }
        if (ChunkUtils.canCopyForward(src, srcOffset, data, destOffset, length)) {
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
        final ByteBuffer byteSrcBuffer = (ByteBuffer) srcBuffer;
        copyFromTypedBuffer(byteSrcBuffer, srcOffset, destOffset, length);
    }

    /**
     * <p>Fill a sub-range of this WritableByteChunk with values from a {@link ByteBuffer}.
     *
     * <p>See {@link #copyFromBuffer(Buffer, int, int, int)} for general documentation.
     *
     * @param srcBuffer  The source {@link ByteBuffer}
     * @param srcOffset  The absolute offset into {@code srcBuffer} to start copying from
     * @param destOffset The offset into this chunk to start copying to
     * @param length     The number of elements to copy
     */
    public final void copyFromTypedBuffer(@NotNull final ByteBuffer srcBuffer, final int srcOffset, final int destOffset, final int length) {
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
    public final ChunkFiller getChunkFiller() {
        return ByteChunkFiller.INSTANCE;
    }

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
    public static <ATTR extends Any, ATTR_DERIV extends ATTR> WritableByteChunk<ATTR> upcast(WritableByteChunk<ATTR_DERIV> self) {
        //noinspection unchecked
        return (WritableByteChunk<ATTR>) self;
    }
    // endregion downcast
}
