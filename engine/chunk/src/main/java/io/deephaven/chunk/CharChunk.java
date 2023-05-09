/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.chunk;

import io.deephaven.util.type.ArrayTypeUtils;
import io.deephaven.chunk.attributes.Any;

import org.jetbrains.annotations.NotNull;

// region ApplyDecoderImports
// endregion ApplyDecoderImports

// region BufferImports
import java.nio.Buffer;
import java.nio.CharBuffer;
// endregion BufferImports

// region BinarySearchImports
import java.util.Arrays;
// endregion BinarySearchImports

/**
 * {@link Chunk} implementation for char data.
 */
public class CharChunk<ATTR extends Any> extends ChunkBase<ATTR> {

    @SuppressWarnings("rawtypes")
    private static final CharChunk EMPTY = new CharChunk<>(ArrayTypeUtils.EMPTY_CHAR_ARRAY, 0, 0);

    public static <ATTR extends Any> CharChunk<ATTR> getEmptyChunk() {
        //noinspection unchecked
        return EMPTY;
    }

    @SuppressWarnings("rawtypes")
    private static final CharChunk[] EMPTY_CHAR_CHUNK_ARRAY = new CharChunk[0];

    static <ATTR extends Any> CharChunk<ATTR>[] getEmptyChunkArray() {
        //noinspection unchecked
        return EMPTY_CHAR_CHUNK_ARRAY;
    }

    // region makeArray
    public static char[] makeArray(int capacity) {
        if (capacity == 0) {
            return ArrayTypeUtils.EMPTY_CHAR_ARRAY;
        }
        return new char[capacity];
    }
    // endregion makeArray

    public static <ATTR extends Any> CharChunk<ATTR> chunkWrap(char[] data) {
        return chunkWrap(data, 0, data.length);
    }

    public static <ATTR extends Any> CharChunk<ATTR> chunkWrap(char[] data, int offset, int capacity) {
        return new CharChunk<>(data, offset, capacity);
    }

    char[] data;

    protected CharChunk(char[] data, int offset, int capacity) {
        super(data.length, offset, capacity);
        this.data = data;
    }

    public final ChunkType getChunkType() {
        return ChunkType.Char;
    }

    public final char get(int index) {
        return data[offset + index];
    }

    @Override
    public CharChunk<ATTR> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new CharChunk<>(data, this.offset + offset, capacity);
    }

    @Override
    public final void copyToChunk(int srcOffset, WritableChunk<? super ATTR> dest, int destOffset, int length) {
        final WritableCharChunk<? super ATTR> wDest = dest.asWritableCharChunk();
        copyToTypedArray(srcOffset, wDest.data, wDest.offset + destOffset, length);
    }

    @Override
    public final void copyToArray(int srcOffset, Object dest, int destOffset, int length) {
        final char[] realType = (char[])dest;
        copyToTypedArray(srcOffset, realType, destOffset, length);
    }

    public final void copyToTypedArray(int srcOffset, char[] destData, int destOffset, int length) {
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
    // endregion ApplyDecoder

    // region CopyToBuffer
    @Override
    public final void copyToBuffer(final int srcOffset, @NotNull final Buffer destBuffer, final int destOffset, final int length) {
        final CharBuffer charDestBuffer = (CharBuffer) destBuffer;
        copyToTypedBuffer(srcOffset, charDestBuffer, destOffset, length);
    }

    /**
     * <p>Copy a sub-range of this CharChunk to a {@link CharBuffer}.
     *
     * <p>See {@link #copyToBuffer(int, Buffer, int, int)} for general documentation.
     *
     * @param srcOffset  The offset into this chunk to start copying from
     * @param destBuffer The destination {@link CharBuffer}
     * @param destOffset The absolute offset into {@code destBuffer} to start copying to
     * @param length     The number of elements to copy
     */
    public final void copyToTypedBuffer(final int srcOffset, @NotNull final CharBuffer destBuffer, final int destOffset, final int length) {
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
    public static <ATTR extends Any, ATTR_DERIV extends ATTR> CharChunk<ATTR_DERIV> downcast(CharChunk<ATTR> self) {
        //noinspection unchecked
        return (CharChunk<ATTR_DERIV>) self;
    }
    // endregion downcast

    // region BinarySearch
    /**
     * Search for {@code key} in this chunk in the index range [0, {@link #size() size}) using Java's primitive
     * ordering. This chunk must be sorted as by {@link WritableCharChunk#sort()} prior to this call.
     * <p>
     * This method does <em>not</em> compare {@code null} or {@code NaN} values according to Deephaven ordering rules.
     *
     * @param key The key to search for
     * @return The index of the key in this chunk, or else {@code (-(insertion point) - 1)} as defined by
     *         {@link Arrays#binarySearch(char[], char)}
     */
    public final int binarySearch(final char key) {
        return Arrays.binarySearch(data, offset, offset + size, key);
    }

    /**
     * Search for {@code key} in this chunk in the index range {@code [fromIndexInclusive, toIndexExclusive)} using
     * Java's primitive ordering. This chunk must be sorted over the search index range as by
     * {@link WritableCharChunk#sort(int, int)} prior to this call.
     * <p>
     * This method does <em>not</em> compare {@code null} or {@code NaN} values according to Deephaven ordering rules.
     *
     * @param fromIndexInclusive The first index to be searched
     * @param toIndexExclusive The index after the last index to be searched
     * @param key The key to search for
     * @return The index of the key in this chunk, or else {@code (-(insertion point) - 1)} as defined by
     *         {@link Arrays#binarySearch(char[], int, int, char)}
     */
    public final int binarySearch(final int fromIndexInclusive, final int toIndexExclusive, final char key) {
        return Arrays.binarySearch(data, offset + fromIndexInclusive, offset + toIndexExclusive, key);
    }
    // endregion BinarySearch
}
