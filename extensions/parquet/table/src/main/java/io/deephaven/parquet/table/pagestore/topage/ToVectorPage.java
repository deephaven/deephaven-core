package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.vector.Vector;
import io.deephaven.chunk.ChunkType;
import io.deephaven.parquet.base.DataWithOffsets;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Array;
import java.nio.IntBuffer;

public class ToVectorPage<ATTR extends Any, RESULT, VECTOR_TYPE extends Vector<?>>
        extends ToPage.Wrap<ATTR, RESULT, VECTOR_TYPE[]> {

    private final Class<VECTOR_TYPE> nativeType;

    public static <ATTR extends Any> ToPage<ATTR, ?> create(@NotNull final Class<?> nativeType,
            @NotNull final Class<?> componentType,
            @NotNull final ToPage<ATTR, ?> toPage) {
        if (!Vector.class.isAssignableFrom(nativeType)) {
            throw new IllegalArgumentException("Native type " + nativeType + " is not a Vector type.");
        }

        final Class<?> columnComponentType = toPage.getNativeComponentType();
        if (!componentType.isAssignableFrom(columnComponentType)) {
            throw new IllegalArgumentException("The component type " + componentType.getCanonicalName() + " for the" +
                    " array type " + nativeType.getCanonicalName() +
                    " is not compatible with the column's component type " + columnComponentType);
        }

        // noinspection rawtypes,unchecked
        return new ToVectorPage(nativeType, toPage);
    }

    private ToVectorPage(@NotNull final Class<VECTOR_TYPE> nativeType, @NotNull final ToPage<ATTR, RESULT> toPage) {
        super(toPage);
        this.nativeType = nativeType;
    }

    @Override
    @NotNull
    public final Class<VECTOR_TYPE> getNativeType() {
        return nativeType;
    }

    @Override
    @NotNull
    public final ChunkType getChunkType() {
        return ChunkType.Object;
    }

    @NotNull
    @Override
    public final VECTOR_TYPE[] convertResult(final Object object) {
        final DataWithOffsets dataWithOffsets = (DataWithOffsets) object;

        // noinspection unchecked
        final VECTOR_TYPE dataWrapper =
                (VECTOR_TYPE) toPage.makeVector(toPage.convertResult(dataWithOffsets.materializeResult));
        final IntBuffer offsets = dataWithOffsets.offsets;

        // noinspection unchecked
        final VECTOR_TYPE[] to = (VECTOR_TYPE[]) Array.newInstance(nativeType, offsets.remaining());

        int lastOffset = 0;
        for (int vi = 0; vi < to.length; ++vi) {
            final int nextOffset = offsets.get();
            if (nextOffset == DataWithOffsets.NULL_OFFSET) {
                to[vi] = null;
            } else {
                // noinspection unchecked
                to[vi] = (VECTOR_TYPE) dataWrapper.subVector(lastOffset, nextOffset).getDirect();
                lastOffset = nextOffset;
            }
        }

        return to;
    }
}
