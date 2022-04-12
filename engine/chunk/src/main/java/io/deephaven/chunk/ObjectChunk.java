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

/**
 * {@link Chunk} implementation for Object data.
 */
public class ObjectChunk<T, ATTR extends Any> extends ChunkBase<ATTR> {

    private static final ObjectChunk EMPTY = new ObjectChunk<>(ArrayTypeUtils.EMPTY_OBJECT_ARRAY, 0, 0);

    public static <T, ATTR extends Any> ObjectChunk<T, ATTR> getEmptyChunk() {
        //noinspection unchecked
        return EMPTY;
    }

    private static final ObjectChunk[] EMPTY_OBJECT_CHUNK_ARRAY = new ObjectChunk[0];

    static <T, ATTR extends Any> ObjectChunk<T, ATTR>[] getEmptyChunkArray() {
        //noinspection unchecked
        return EMPTY_OBJECT_CHUNK_ARRAY;
    }

    // region makeArray
    public static <T> T[] makeArray(int capacity) {
        if (capacity == 0) {
            //noinspection unchecked
            return (T[]) ArrayTypeUtils.EMPTY_OBJECT_ARRAY;
        }
        //noinspection unchecked
        return (T[])new Object[capacity];
    }
    // endregion makeArray

    public static <T, ATTR extends Any> ObjectChunk<T, ATTR> chunkWrap(T[] data) {
        return chunkWrap(data, 0, data.length);
    }

    public static <T, ATTR extends Any> ObjectChunk<T, ATTR> chunkWrap(T[] data, int offset, int capacity) {
        return new ObjectChunk<>(data, offset, capacity);
    }

    T[] data;

    protected ObjectChunk(T[] data, int offset, int capacity) {
        super(data.length, offset, capacity);
        this.data = data;
    }

    public final ChunkType getChunkType() {
        return ChunkType.Object;
    }

    public final T get(int index) {
        return data[offset + index];
    }

    @Override
    public ObjectChunk<T, ATTR> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new ObjectChunk<>(data, this.offset + offset, capacity);
    }

    @Override
    public final void copyToChunk(int srcOffset, WritableChunk<? super ATTR> dest, int destOffset, int length) {
        final WritableObjectChunk<T, ? super ATTR> wDest = dest.asWritableObjectChunk();
        copyToTypedArray(srcOffset, wDest.data, wDest.offset + destOffset, length);
    }

    @Override
    public final void copyToArray(int srcOffset, Object dest, int destOffset, int length) {
        final T[] realType = (T[])dest;
        copyToTypedArray(srcOffset, realType, destOffset, length);
    }

    public final void copyToTypedArray(int srcOffset, T[] destData, int destOffset, int length) {
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
    // endregion CopyToBuffer

    // region downcast
    public <T_DERIV extends T> ObjectChunk<T_DERIV, ATTR> asTypedObjectChunk() {
        //noinspection unchecked
        return (ObjectChunk<T_DERIV, ATTR>) this;
    }

    public static <T, ATTR extends Any, ATTR_DERIV extends ATTR> WritableObjectChunk<T, ATTR_DERIV> downcast(WritableObjectChunk<T, ATTR> self) {
        //noinspection unchecked
        return (WritableObjectChunk<T, ATTR_DERIV>) self;
    }
    // endregion downcast
}
