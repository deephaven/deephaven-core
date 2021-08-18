/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit WritableCharChunk and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.sources.chunk;

// @formatter:off

import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.util.chunkfillers.BooleanChunkFiller;
import io.deephaven.db.v2.sources.chunk.util.chunkfillers.ChunkFiller;
import io.deephaven.db.v2.sources.chunk.util.pools.MultiChunkPool;
import io.deephaven.db.v2.utils.ChunkUtils;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
// region FillWithNullValueImports
// endregion FillWithNullValueImports

// region BufferImports
// endregion BufferImports

// @formatter:on

/**
 * {@link WritableChunk} implementation for boolean data.
 */
public class WritableBooleanChunk<ATTR extends Any> extends BooleanChunk<ATTR> implements WritableChunk<ATTR> {

    private static final WritableBooleanChunk[] EMPTY_WRITABLE_BOOLEAN_CHUNK_ARRAY = new WritableBooleanChunk[0];

    static <ATTR extends Any> WritableBooleanChunk<ATTR>[] getEmptyChunkArray() {
        //noinspection unchecked
        return EMPTY_WRITABLE_BOOLEAN_CHUNK_ARRAY;
    }

    public static <ATTR extends Any> WritableBooleanChunk<ATTR> makeWritableChunk(int size) {
        return MultiChunkPool.forThisThread().getBooleanChunkPool().takeWritableBooleanChunk(size);
    }

    public static WritableBooleanChunk makeWritableChunkForPool(int size) {
        return new WritableBooleanChunk(makeArray(size), 0, size) {
            @Override
            public void close() {
                MultiChunkPool.forThisThread().getBooleanChunkPool().giveWritableBooleanChunk(this);
            }
        };
    }

    public static <ATTR extends Any> WritableBooleanChunk<ATTR> writableChunkWrap(boolean[] data) {
        return writableChunkWrap(data, 0, data.length);
    }

    public static <ATTR extends Any> WritableBooleanChunk<ATTR> writableChunkWrap(boolean[] data, int offset, int size) {
        return new WritableBooleanChunk<>(data, offset, size);
    }

    WritableBooleanChunk(boolean[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    public final void set(int index, boolean value) {
        data[offset + index] = value;
    }

    public final void add(boolean value) { data[offset + size++] = value; }

    @Override
    public WritableBooleanChunk<ATTR> slice(int offset, int capacity) {
        ChunkUtils.checkSliceArgs(size, offset, capacity);
        return new WritableBooleanChunk<>(data, this.offset + offset, capacity);
    }

    // region FillWithNullValueImpl
    // endregion FillWithNullValueImpl

    // region fillWithBoxedValue
    @Override
    public final void fillWithBoxedValue(int offset, int size, Object value) {
        fillWithValue(offset,size, TypeUtils.unbox((Boolean) value));
    }
    // endregion fillWithBoxedValue

    public final void fillWithValue(final int offset, final int length, final boolean value) {
        final int netOffset = this.offset + offset;
        if (length >= SYSTEM_ARRAYFILL_THRESHOLD) {
            Arrays.fill(data, netOffset, netOffset + length, value);
            return;
        }
        for (int ii = 0; ii < length; ++ii) {
            data[netOffset + ii] = value;
        }
    }

    public final void appendTypedChunk(BooleanChunk<? extends ATTR> src, int srcOffset, int length) {
        copyFromTypedChunk(src, srcOffset, size, length);
        size += length;
    }

    @Override
    public final void copyFromChunk(Chunk<? extends ATTR> src, int srcOffset, int destOffset, int length) {
        final BooleanChunk<? extends ATTR> typedSrc = src.asBooleanChunk();
        copyFromTypedChunk(typedSrc, srcOffset, destOffset, length);
    }

    public final void copyFromTypedChunk(BooleanChunk<? extends ATTR> src, int srcOffset, int destOffset, int length) {
        copyFromTypedArray(src.data, src.offset + srcOffset, destOffset, length);
    }

    @Override
    public final void copyFromArray(Object srcArray, int srcOffset, int destOffset, int length) {
        final boolean[] typedArray = (boolean[])srcArray;
        copyFromTypedArray(typedArray, srcOffset, destOffset, length);
    }

    public final void copyFromTypedArray(boolean[] src, int srcOffset, int destOffset, int length) {
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
    // endregion CopyFromBuffer

    @Override
    public final ChunkFiller getChunkFiller() {
        return BooleanChunkFiller.INSTANCE;
    }

    @Override
    public final void sort() {
        sort(0, size);
    }

    // region sort
    // endregion sort

    @Override
    public void close() {
    }

    // region downcast
    public static <ATTR extends Any, ATTR_DERIV extends ATTR> WritableBooleanChunk<ATTR> upcast(WritableBooleanChunk<ATTR_DERIV> self) {
        //noinspection unchecked
        return (WritableBooleanChunk<ATTR>) self;
    }
    // endregion downcast
}
