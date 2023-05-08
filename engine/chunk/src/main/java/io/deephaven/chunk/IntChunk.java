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
import java.nio.IntBuffer;
// endregion BufferImports

// region BinarySearchImports
import java.util.Arrays;
// endregion BinarySearchImports

/**
 * {@link Chunk} implementation for int data.
 */
public class IntChunk<ATTR extends Any> extends ChunkBase<ATTR> {

    @SuppressWarnings("rawtypes")
    private static final IntChunk EMPTY = new IntChunk<>(ArrayTypeUtils.EMPTY_INT_ARRAY, 0, 0);

    public static <ATTR extends Any> IntChunk<ATTR> getEmptyChunk() {
        //noinspection unchecked
        return EMPTY;
    }

    @SuppressWarnings("rawtypes")
    private static final IntChunk[] EMPTY_INT_CHUNK_ARRAY = new IntChunk[0];

    static <ATTR extends Any> IntChunk<ATTR>[] getEmptyChunkArray() {
        //noinspection unchecked
        return EMPTY_INT_CHUNK_ARRAY;
    }

    // region makeArray
    public static int[] makeArray(int capacity) {
        if (capacity == 0) {
            return ArrayTypeUtils.EMPTY_INT_ARRAY;
        }
        return new int[capacity];
    }
    // endregion makeArray

    public static <ATTR extends Any> IntChunk<ATTR> chunkWrap(int[] data) {
        return chunkWrap(data, 0, data.length);
    }

    public static <ATTR extends Any> IntChunk<ATTR> chunkWrap(int[] data, int offset, int capacity) {
        return new IntChunk<>(data, offset, capacity);
    }

    int[] data;

    protected IntChunk(int[] data, int offset, int capacity) {
        super(data.length, offset, capacity);
        this.data = data;
    }

    public final ChunkType getChunkType() {
        return ChunkType.Int;
    }

    public final int get(int index) {
        return data[offset + index];
    }

    @Override
    public IntChunk<ATTR> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new IntChunk<>(data, this.offset + offset, capacity);
    }

    @Override
    public final void copyToChunk(int srcOffset, WritableChunk<? super ATTR> dest, int destOffset, int length) {
        final WritableIntChunk<? super ATTR> wDest = dest.asWritableIntChunk();
        copyToTypedArray(srcOffset, wDest.data, wDest.offset + destOffset, length);
    }

    @Override
    public final void copyToArray(int srcOffset, Object dest, int destOffset, int length) {
        final int[] realType = (int[])dest;
        copyToTypedArray(srcOffset, realType, destOffset, length);
    }

    public final void copyToTypedArray(int srcOffset, int[] destData, int destOffset, int length) {
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
        final IntBuffer intDestBuffer = (IntBuffer) destBuffer;
        copyToTypedBuffer(srcOffset, intDestBuffer, destOffset, length);
    }

    /**
     * <p>Copy a sub-range of this IntChunk to a {@link IntBuffer}.
     *
     * <p>See {@link #copyToBuffer(int, Buffer, int, int)} for general documentation.
     *
     * @param srcOffset  The offset into this chunk to start copying from
     * @param destBuffer The destination {@link IntBuffer}
     * @param destOffset The absolute offset into {@code destBuffer} to start copying to
     * @param length     The number of elements to copy
     */
    public final void copyToTypedBuffer(final int srcOffset, @NotNull final IntBuffer destBuffer, final int destOffset, final int length) {
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
    public static <ATTR extends Any, ATTR_DERIV extends ATTR> IntChunk<ATTR_DERIV> downcast(IntChunk<ATTR> self) {
        //noinspection unchecked
        return (IntChunk<ATTR_DERIV>) self;
    }
    // endregion downcast

    // region BinarySearch
    /**
     * Search for {@code key} in this chunk in the index range [0, {@link #size() size}) using Java's primitive
     * ordering. This chunk must be sorted as by {@link WritableIntChunk#sort()} prior to this call.
     * <p>
     * This method does <em>not</em> compare {@code null} or {@code NaN} values according to Deephaven ordering rules.
     *
     * @param key The key to search for
     * @return The index of the key in this chunk, or else {@code (-(insertion point) - 1)} as defined by
     *         {@link Arrays#binarySearch(int[], int)}
     */
    public final int binarySearch(final int key) {
        return Arrays.binarySearch(data, offset, offset + size, key);
    }

    /**
     * Search for {@code key} in this chunk in the index range {@code [fromIndexInclusive, toIndexExclusive)} using
     * Java's primitive ordering. This chunk must be sorted over the search index range as by
     * {@link WritableIntChunk#sort(int, int)} prior to this call.
     * <p>
     * This method does <em>not</em> compare {@code null} or {@code NaN} values according to Deephaven ordering rules.
     *
     * @param fromIndexInclusive The first index to be searched
     * @param toIndexExclusive The index after the last index to be searched
     * @param key The key to search for
     * @return The index of the key in this chunk, or else {@code (-(insertion point) - 1)} as defined by
     *         {@link Arrays#binarySearch(int[], int, int, int)}
     */
    public final int binarySearch(final int fromIndexInclusive, final int toIndexExclusive, final int key) {
        return Arrays.binarySearch(data, offset + fromIndexInclusive, offset + toIndexExclusive, key);
    }
    // endregion BinarySearch
}
