//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.parquet.base.materializers.LocalDateMaterializer;
import org.jetbrains.annotations.NotNull;

import java.time.LocalDate;

public class ToLocalDatePage<ATTR extends Any> implements ToPage<ATTR, LocalDate[]> {

    @SuppressWarnings("rawtypes")
    private static final ToLocalDatePage INSTANCE = new ToLocalDatePage<>();

    public static <ATTR extends Any> ToLocalDatePage<ATTR> create(final Class<?> nativeType) {
        if (nativeType == null || LocalDate.class.equals(nativeType)) {
            // noinspection unchecked
            return INSTANCE;
        }
        throw new IllegalArgumentException(
                "The native type for a LocalDate column is " + nativeType.getCanonicalName());
    }

    private ToLocalDatePage() {}

    @Override
    @NotNull
    public final Class<LocalDate> getNativeType() {
        return LocalDate.class;
    }

    @Override
    @NotNull
    public final ChunkType getChunkType() {
        return ChunkType.Object;
    }

    @Override
    @NotNull
    public final PageMaterializerFactory getPageMaterializerFactory() {
        return LocalDateMaterializer.FACTORY;
    }
}
