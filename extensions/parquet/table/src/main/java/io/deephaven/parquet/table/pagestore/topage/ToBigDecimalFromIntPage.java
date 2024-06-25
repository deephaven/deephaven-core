//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit ToBigDecimalFromLongPage and run "./gradlew replicateToPage" to regenerate
//
// @formatter:off
package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.parquet.base.materializers.BigDecimalFromIntMaterializer;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;

import static io.deephaven.util.QueryConstants.NULL_INT_BOXED;

public class ToBigDecimalFromIntPage<ATTR extends Any> extends ToBigDecimalBase<ATTR> {

    public static <ATTR extends Any> ToPage<ATTR, BigDecimal[]> create(
            @NotNull final Class<?> nativeType,
            final int precision,
            final int scale) {
        return new ToBigDecimalFromIntPage<>(nativeType, precision, scale);
    }

    private ToBigDecimalFromIntPage(@NotNull final Class<?> nativeType, final int precision, final int scale) {
        super(nativeType, precision, scale);
    }

    @Override
    @NotNull
    public final Object nullValue() {
        return NULL_INT_BOXED;
    }

    @Override
    @NotNull
    public final PageMaterializerFactory getPageMaterializerFactory() {
        return new BigDecimalFromIntMaterializer.Factory(scale);
    }
}
