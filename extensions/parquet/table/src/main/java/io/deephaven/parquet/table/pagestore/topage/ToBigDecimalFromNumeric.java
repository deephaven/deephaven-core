//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.parquet.base.materializers.BigDecimalFromIntMaterializer;
import io.deephaven.parquet.base.materializers.BigDecimalFromLongMaterializer;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;

public class ToBigDecimalFromNumeric<ATTR extends Any> implements ToPage<ATTR, BigDecimal[]> {

    public static <ATTR extends Any> ToPage<ATTR, BigDecimal[]> createFromInt(
            @NotNull final Class<?> nativeType,
            final int scale) {
        return new ToBigDecimalFromNumeric<>(nativeType, new BigDecimalFromIntMaterializer.Factory(scale));
    }

    public static <ATTR extends Any> ToPage<ATTR, BigDecimal[]> createFromLong(
            @NotNull final Class<?> nativeType,
            final int scale) {
        return new ToBigDecimalFromNumeric<>(nativeType, new BigDecimalFromLongMaterializer.Factory(scale));
    }

    private final PageMaterializerFactory pageMaterializerFactory;

    private ToBigDecimalFromNumeric(
            @NotNull final Class<?> nativeType,
            @NotNull final PageMaterializerFactory pageMaterializerFactory) {
        if (!BigDecimal.class.equals(nativeType)) {
            throw new IllegalArgumentException(
                    "The native type for a BigDecimal column is " + nativeType.getCanonicalName());
        }
        this.pageMaterializerFactory = pageMaterializerFactory;
    }

    @NotNull
    @Override
    public final Class<?> getNativeType() {
        return BigDecimal.class;
    }

    @Override
    @NotNull
    public final ChunkType getChunkType() {
        return ChunkType.Object;
    }

    @Override
    @NotNull
    public final PageMaterializerFactory getPageMaterializerFactory() {
        return pageMaterializerFactory;
    }
}
