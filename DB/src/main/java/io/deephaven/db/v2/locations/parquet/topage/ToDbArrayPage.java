package io.deephaven.db.v2.locations.parquet.topage;

import io.deephaven.db.tables.dbarrays.DbArrayBase;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.parquet.DataWithOffsets;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Array;
import java.nio.IntBuffer;
import java.util.Arrays;
import java.util.function.Function;

public class ToDbArrayPage<ATTR extends Attributes.Any, RESULT> extends ToPage.Wrap<ATTR, RESULT, DbArrayBase[]> {

    private final Class<?> nativeType;

    public static <ATTR extends Attributes.Any>
    ToPage<ATTR, DbArrayBase[]> create(@NotNull Class<?> nativeType, @NotNull Class<?> componentType, ToPage<ATTR, ?> toPage) {
        Class<?> columnComponentType = toPage.getNativeComponentType();

        if (!DbArrayBase.class.isAssignableFrom(nativeType)) {
            throw new IllegalArgumentException("Native type " + nativeType + " is not a DbArray type.");
        }

        if (!componentType.isAssignableFrom(columnComponentType)) {
            throw new IllegalArgumentException("The component type, " + componentType.getCanonicalName() + ", for the" +
                    " native array type " + nativeType.getCanonicalName() +
                    " is not compatible with the column's component type " + columnComponentType);
        }

        return new ToDbArrayPage<>(nativeType, toPage);
    }

    private ToDbArrayPage(Class<?> nativeType, ToPage<ATTR, RESULT> toPage) {
        super(toPage);
        this.nativeType = nativeType;
    }

    @Override
    @NotNull
    public final Class getNativeType() {
        return nativeType;
    }

    @Override
    @NotNull
    public final ChunkType getChunkType() {
        return ChunkType.Object;
    }

    @NotNull
    @Override
    public final DbArrayBase[] convertResult(final Object object) {
        final DataWithOffsets dataWithOffsets = (DataWithOffsets) object;

        final DbArrayBase dbArrayBase = toPage.makeDbArray(toPage.convertResult(dataWithOffsets.materializeResult));
        //noinspection unchecked
        final Function<DbArrayBase, ? extends DbArrayBase> getDirectFunction = DbArrayBase.resolveGetDirect((Class<DbArrayBase>) dbArrayBase.getClass());
        final IntBuffer offsets = dataWithOffsets.offsets;

        final DbArrayBase[] to = (DbArrayBase []) Array.newInstance(nativeType, offsets.remaining());

        int lastOffset = 0;
        for (int vi = 0; vi < to.length; ++vi) {
            final int nextOffset = offsets.get();
            if (nextOffset == DataWithOffsets.NULL_OFFSET) {
                to[vi] = null;
            } else {
                to[vi] = getDirectFunction.apply(dbArrayBase.subArray(lastOffset, nextOffset));
                lastOffset = nextOffset;
            }
        }

        return to;
    }
}
