//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.parquet.base.materializers.IntMaterializer;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_INT_BOXED;

public class ToIntPage<ATTR extends Any> implements ToPage<ATTR, int[]> {

    @SuppressWarnings("rawtypes")
    private static final ToIntPage INSTANCE = new ToIntPage<>();

    public static <ATTR extends Any> ToIntPage<ATTR> create(Class<?> nativeType) {
        if (nativeType == null || int.class.equals(nativeType)) {
            // noinspection unchecked
            return INSTANCE;
        }

        throw new IllegalArgumentException("The native type for a Int column is " + nativeType.getCanonicalName());
    }

    private ToIntPage() {}

    @Override
    @NotNull
    public final Class<Integer> getNativeType() {
        return int.class;
    }

    @Override
    @NotNull
    public final ChunkType getChunkType() {
        return ChunkType.Int;
    }

    @Override
    @NotNull
    public final Object nullValue() {
        return NULL_INT_BOXED;
    }

    @Override
    @NotNull
    public final PageMaterializerFactory getPageMaterializerFactory() {
        return IntMaterializer.FACTORY;
    }
}
