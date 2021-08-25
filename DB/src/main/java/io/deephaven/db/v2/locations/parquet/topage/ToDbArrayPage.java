package io.deephaven.db.v2.locations.parquet.topage;

import io.deephaven.db.tables.dbarrays.DbArrayBase;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.parquet.DataWithOffsets;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Array;
import java.nio.IntBuffer;

public class ToDbArrayPage<ATTR extends Attributes.Any, RESULT, ARRAY_TYPE extends DbArrayBase<?>>
        extends ToPage.Wrap<ATTR, RESULT, ARRAY_TYPE[]> {

    private final Class<ARRAY_TYPE> nativeType;

    public static <ATTR extends Attributes.Any> ToPage<ATTR, ?> create(@NotNull final Class<?> nativeType,
            @NotNull final Class<?> componentType,
            @NotNull final ToPage<ATTR, ?> toPage) {
        if (!DbArrayBase.class.isAssignableFrom(nativeType)) {
            throw new IllegalArgumentException("Native type " + nativeType + " is not a DbArray type.");
        }

        final Class<?> columnComponentType = toPage.getNativeComponentType();
        if (!componentType.isAssignableFrom(columnComponentType)) {
            throw new IllegalArgumentException("The component type " + componentType.getCanonicalName() + " for the" +
                    " array type " + nativeType.getCanonicalName() +
                    " is not compatible with the column's component type " + columnComponentType);
        }

        // noinspection rawtypes,unchecked
        return new ToDbArrayPage(nativeType, toPage);
    }

    private ToDbArrayPage(@NotNull final Class<ARRAY_TYPE> nativeType, @NotNull final ToPage<ATTR, RESULT> toPage) {
        super(toPage);
        this.nativeType = nativeType;
    }

    @Override
    @NotNull
    public final Class<ARRAY_TYPE> getNativeType() {
        return nativeType;
    }

    @Override
    @NotNull
    public final ChunkType getChunkType() {
        return ChunkType.Object;
    }

    @NotNull
    @Override
    public final ARRAY_TYPE[] convertResult(final Object object) {
        final DataWithOffsets dataWithOffsets = (DataWithOffsets) object;

        // noinspection unchecked
        final ARRAY_TYPE dataWrapper =
                (ARRAY_TYPE) toPage.makeDbArray(toPage.convertResult(dataWithOffsets.materializeResult));
        final IntBuffer offsets = dataWithOffsets.offsets;

        // noinspection unchecked
        final ARRAY_TYPE[] to = (ARRAY_TYPE[]) Array.newInstance(nativeType, offsets.remaining());

        int lastOffset = 0;
        for (int vi = 0; vi < to.length; ++vi) {
            final int nextOffset = offsets.get();
            if (nextOffset == DataWithOffsets.NULL_OFFSET) {
                to[vi] = null;
            } else {
                // noinspection unchecked
                to[vi] = (ARRAY_TYPE) dataWrapper.subArray(lastOffset, nextOffset).getDirect();
                lastOffset = nextOffset;
            }
        }

        return to;
    }
}
