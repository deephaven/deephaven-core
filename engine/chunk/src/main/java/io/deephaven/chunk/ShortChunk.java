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
// endregion ApplyDecoderImports

// region BufferImports
import java.nio.Buffer;
import java.nio.ShortBuffer;
// endregion BufferImports

// region BinarySearchImports
import java.util.Arrays;
// endregion BinarySearchImports

/**
 * {@link Chunk} implementation for short data.
 */
public class ShortChunk<ATTR extends Any> extends ChunkBase<ATTR> {

    @SuppressWarnings("rawtypes")
    private static final ShortChunk EMPTY = new ShortChunk<>(ArrayTypeUtils.EMPTY_SHORT_ARRAY, 0, 0);

    public static <ATTR extends Any> ShortChunk<ATTR> getEmptyChunk() {
        //noinspection unchecked
        return EMPTY;
    }

    @SuppressWarnings("rawtypes")
    private static final ShortChunk[] EMPTY_SHORT_CHUNK_ARRAY = new ShortChunk[0];

    static <ATTR extends Any> ShortChunk<ATTR>[] getEmptyChunkArray() {
        //noinspection unchecked
        return EMPTY_SHORT_CHUNK_ARRAY;
    }

    // region makeArray
    public static short[] makeArray(int capacity) {
        if (capacity == 0) {
            return ArrayTypeUtils.EMPTY_SHORT_ARRAY;
        }
        return new short[capacity];
    }
    // endregion makeArray

    public static <ATTR extends Any> ShortChunk<ATTR> chunkWrap(short[] data) {
        return chunkWrap(data, 0, data.length);
    }

    public static <ATTR extends Any> ShortChunk<ATTR> chunkWrap(short[] data, int offset, int capacity) {
        return new ShortChunk<>(data, offset, capacity);
    }

    short[] data;

    protected ShortChunk(short[] data, int offset, int capacity) {
        super(data.length, offset, capacity);
        this.data = data;
    }

    public final ChunkType getChunkType() {
        return ChunkType.Short;
    }

    public final short get(int index) {
        return data[offset + index];
    }

    @Override
    public ShortChunk<ATTR> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new ShortChunk<>(data, this.offset + offset, capacity);
    }

    @Override
    public final void copyToChunk(int srcOffset, WritableChunk<? super ATTR> dest, int destOffset, int length) {
        final WritableShortChunk<? super ATTR> wDest = dest.asWritableShortChunk();
        copyToTypedArray(srcOffset, wDest.data, wDest.offset + destOffset, length);
    }

    @Override
    public final void copyToArray(int srcOffset, Object dest, int destOffset, int length) {
        final short[] realType = (short[])dest;
        copyToTypedArray(srcOffset, realType, destOffset, length);
    }

    public final void copyToTypedArray(int srcOffset, short[] destData, int destOffset, int length) {
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
        final ShortBuffer shortDestBuffer = (ShortBuffer) destBuffer;
        copyToTypedBuffer(srcOffset, shortDestBuffer, destOffset, length);
    }

    /**
     * <p>Copy a sub-range of this ShortChunk to a {@link ShortBuffer}.
     *
     * <p>See {@link #copyToBuffer(int, Buffer, int, int)} for general documentation.
     *
     * @param srcOffset  The offset into this chunk to start copying from
     * @param destBuffer The destination {@link ShortBuffer}
     * @param destOffset The absolute offset into {@code destBuffer} to start copying to
     * @param length     The number of elements to copy
     */
    public final void copyToTypedBuffer(final int srcOffset, @NotNull final ShortBuffer destBuffer, final int destOffset, final int length) {
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
    public static <ATTR extends Any, ATTR_DERIV extends ATTR> ShortChunk<ATTR_DERIV> downcast(ShortChunk<ATTR> self) {
        //noinspection unchecked
        return (ShortChunk<ATTR_DERIV>) self;
    }
    // endregion downcast

    // region BinarySearch
    /**
     * Search for {@code key} in this chunk in the index range [0, {@link #size() size}) using Java's primitive
     * ordering. This chunk must be sorted as by {@link WritableShortChunk#sort()} prior to this call.
     * <p>
     * This method does <em>not</em> compare {@code null} or {@code NaN} values according to Deephaven ordering rules.
     *
     * @param key The key to search for
     * @return The index of the key in this chunk, or else {@code (-(insertion point - 1)} as defined by
     *         {@link Arrays#binarySearch(short[], short)}
     */
    public final int binarySearch(final short key) {
        return Arrays.binarySearch(data, offset, offset + size, key);
    }

    /**
     * Search for {@code key} in this chunk in the index range {@code [fromIndexInclusive, toIndexExclusive)} using
     * Java's primitive ordering. This chunk must be sorted over the search index range as by
     * {@link WritableShortChunk#sort(int, int)} prior to this call.
     * <p>
     * This method does <em>not</em> compare {@code null} or {@code NaN} values according to Deephaven ordering rules.
     *
     * @param fromIndexInclusive The first index to be searched
     * @param toIndexExclusive The index after the last index to be searched
     * @param key The key to search for
     * @return The index of the key in this chunk, or else {@code (-(insertion point - 1)} as defined by
     *         {@link Arrays#binarySearch(short[], int, int, short)}
     */
    public final int binarySearch(final int fromIndexInclusive, final int toIndexExclusive, final short key) {
        return Arrays.binarySearch(data, offset + fromIndexInclusive, offset + toIndexExclusive, key);
    }
    // endregion BinarySearch
}
