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
import io.deephaven.util.codec.ObjectDecoder;
// endregion ApplyDecoderImports

// region BufferImports
import java.nio.Buffer;
import java.nio.ByteBuffer;
// endregion BufferImports

// region BinarySearchImports
import java.util.Arrays;
// endregion BinarySearchImports

/**
 * {@link Chunk} implementation for byte data.
 */
public class ByteChunk<ATTR extends Any> extends ChunkBase<ATTR> {

    @SuppressWarnings("rawtypes")
    private static final ByteChunk EMPTY = new ByteChunk<>(ArrayTypeUtils.EMPTY_BYTE_ARRAY, 0, 0);

    public static <ATTR extends Any> ByteChunk<ATTR> getEmptyChunk() {
        //noinspection unchecked
        return EMPTY;
    }

    @SuppressWarnings("rawtypes")
    private static final ByteChunk[] EMPTY_BYTE_CHUNK_ARRAY = new ByteChunk[0];

    static <ATTR extends Any> ByteChunk<ATTR>[] getEmptyChunkArray() {
        //noinspection unchecked
        return EMPTY_BYTE_CHUNK_ARRAY;
    }

    // region makeArray
    public static byte[] makeArray(int capacity) {
        if (capacity == 0) {
            return ArrayTypeUtils.EMPTY_BYTE_ARRAY;
        }
        return new byte[capacity];
    }
    // endregion makeArray

    public static <ATTR extends Any> ByteChunk<ATTR> chunkWrap(byte[] data) {
        return chunkWrap(data, 0, data.length);
    }

    public static <ATTR extends Any> ByteChunk<ATTR> chunkWrap(byte[] data, int offset, int capacity) {
        return new ByteChunk<>(data, offset, capacity);
    }

    byte[] data;

    protected ByteChunk(byte[] data, int offset, int capacity) {
        super(data.length, offset, capacity);
        this.data = data;
    }

    public final ChunkType getChunkType() {
        return ChunkType.Byte;
    }

    public final byte get(int index) {
        return data[offset + index];
    }

    @Override
    public ByteChunk<ATTR> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new ByteChunk<>(data, this.offset + offset, capacity);
    }

    @Override
    public final void copyToChunk(int srcOffset, WritableChunk<? super ATTR> dest, int destOffset, int length) {
        final WritableByteChunk<? super ATTR> wDest = dest.asWritableByteChunk();
        copyToTypedArray(srcOffset, wDest.data, wDest.offset + destOffset, length);
    }

    @Override
    public final void copyToArray(int srcOffset, Object dest, int destOffset, int length) {
        final byte[] realType = (byte[])dest;
        copyToTypedArray(srcOffset, realType, destOffset, length);
    }

    public final void copyToTypedArray(int srcOffset, byte[] destData, int destOffset, int length) {
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
    public final <T> T applyDecoder(ObjectDecoder<T> decoder) {
        return decoder.decode(data, offset, size);
    }

    public final <T> T applyDecoder(ObjectDecoder<T> decoder, int offsetSrc, int length) {
        return decoder.decode(data, offset + offsetSrc, length);
    }
    // endregion ApplyDecoder

    // region CopyToBuffer
    @Override
    public final void copyToBuffer(final int srcOffset, @NotNull final Buffer destBuffer, final int destOffset, final int length) {
        final ByteBuffer byteDestBuffer = (ByteBuffer) destBuffer;
        copyToTypedBuffer(srcOffset, byteDestBuffer, destOffset, length);
    }

    /**
     * <p>Copy a sub-range of this ByteChunk to a {@link ByteBuffer}.
     *
     * <p>See {@link #copyToBuffer(int, Buffer, int, int)} for general documentation.
     *
     * @param srcOffset  The offset into this chunk to start copying from
     * @param destBuffer The destination {@link ByteBuffer}
     * @param destOffset The absolute offset into {@code destBuffer} to start copying to
     * @param length     The number of elements to copy
     */
    public final void copyToTypedBuffer(final int srcOffset, @NotNull final ByteBuffer destBuffer, final int destOffset, final int length) {
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
    public static <ATTR extends Any, ATTR_DERIV extends ATTR> ByteChunk<ATTR_DERIV> downcast(ByteChunk<ATTR> self) {
        //noinspection unchecked
        return (ByteChunk<ATTR_DERIV>) self;
    }
    // endregion downcast

    // region BinarySearch
    /**
     * Search for {@code key} in this chunk in the index range [0, {@link #size() size}) using Java's primitive
     * ordering. This chunk must be sorted as by {@link WritableByteChunk#sort()} prior to this call.
     * <p>
     * This method does <em>not</em> compare {@code null} or {@code NaN} values according to Deephaven ordering rules.
     *
     * @param key The key to search for
     * @return The index of the key in this chunk, or else {@code (-(insertion point - 1)} as defined by
     *         {@link Arrays#binarySearch(byte[], byte)}
     */
    public final int binarySearch(final byte key) {
        return Arrays.binarySearch(data, offset, offset + size, key);
    }

    /**
     * Search for {@code key} in this chunk in the index range {@code [fromIndexInclusive, toIndexExclusive)} using
     * Java's primitive ordering. This chunk must be sorted over the search index range as by
     * {@link WritableByteChunk#sort(int, int)} prior to this call.
     * <p>
     * This method does <em>not</em> compare {@code null} or {@code NaN} values according to Deephaven ordering rules.
     *
     * @param fromIndexInclusive The first index to be searched
     * @param toIndexExclusive The index after the last index to be searched
     * @param key The key to search for
     * @return The index of the key in this chunk, or else {@code (-(insertion point - 1)} as defined by
     *         {@link Arrays#binarySearch(byte[], int, int, byte)}
     */
    public final int binarySearch(final int fromIndexInclusive, final int toIndexExclusive, final byte key) {
        return Arrays.binarySearch(data, offset + fromIndexInclusive, offset + toIndexExclusive, key);
    }
    // endregion BinarySearch
}
