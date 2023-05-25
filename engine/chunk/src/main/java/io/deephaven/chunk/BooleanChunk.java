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
// endregion BufferImports

// region BinarySearchImports
// endregion BinarySearchImports

/**
 * {@link Chunk} implementation for boolean data.
 */
public class BooleanChunk<ATTR extends Any> extends ChunkBase<ATTR> {

    @SuppressWarnings("rawtypes")
    private static final BooleanChunk EMPTY = new BooleanChunk<>(ArrayTypeUtils.EMPTY_BOOLEAN_ARRAY, 0, 0);

    public static <ATTR extends Any> BooleanChunk<ATTR> getEmptyChunk() {
        //noinspection unchecked
        return EMPTY;
    }

    @SuppressWarnings("rawtypes")
    private static final BooleanChunk[] EMPTY_BOOLEAN_CHUNK_ARRAY = new BooleanChunk[0];

    static <ATTR extends Any> BooleanChunk<ATTR>[] getEmptyChunkArray() {
        //noinspection unchecked
        return EMPTY_BOOLEAN_CHUNK_ARRAY;
    }

    // region makeArray
    public static boolean[] makeArray(int capacity) {
        if (capacity == 0) {
            return ArrayTypeUtils.EMPTY_BOOLEAN_ARRAY;
        }
        return new boolean[capacity];
    }
    // endregion makeArray

    public static <ATTR extends Any> BooleanChunk<ATTR> chunkWrap(boolean[] data) {
        return chunkWrap(data, 0, data.length);
    }

    public static <ATTR extends Any> BooleanChunk<ATTR> chunkWrap(boolean[] data, int offset, int capacity) {
        return new BooleanChunk<>(data, offset, capacity);
    }

    boolean[] data;

    protected BooleanChunk(boolean[] data, int offset, int capacity) {
        super(data.length, offset, capacity);
        this.data = data;
    }

    public final ChunkType getChunkType() {
        return ChunkType.Boolean;
    }

    public final boolean get(int index) {
        return data[offset + index];
    }

    @Override
    public BooleanChunk<ATTR> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new BooleanChunk<>(data, this.offset + offset, capacity);
    }

    @Override
    public final void copyToChunk(int srcOffset, WritableChunk<? super ATTR> dest, int destOffset, int length) {
        final WritableBooleanChunk<? super ATTR> wDest = dest.asWritableBooleanChunk();
        copyToTypedArray(srcOffset, wDest.data, wDest.offset + destOffset, length);
    }

    @Override
    public final void copyToArray(int srcOffset, Object dest, int destOffset, int length) {
        final boolean[] realType = (boolean[])dest;
        copyToTypedArray(srcOffset, realType, destOffset, length);
    }

    public final void copyToTypedArray(int srcOffset, boolean[] destData, int destOffset, int length) {
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
    // endregion CopyToBuffer

    // region downcast
    public static <ATTR extends Any, ATTR_DERIV extends ATTR> BooleanChunk<ATTR_DERIV> downcast(BooleanChunk<ATTR> self) {
        //noinspection unchecked
        return (BooleanChunk<ATTR_DERIV>) self;
    }
    // endregion downcast

    // region BinarySearch
    // endregion BinarySearch
}
