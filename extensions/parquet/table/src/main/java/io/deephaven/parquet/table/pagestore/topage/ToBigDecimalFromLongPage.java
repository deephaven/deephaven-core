//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.parquet.base.materializers.BigDecimalFromLongMaterializer;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;

import static io.deephaven.util.QueryConstants.NULL_LONG_BOXED;

public class ToBigDecimalFromLongPage<ATTR extends Any> extends ToBigDecimalBase<ATTR> {

    public static <ATTR extends Any> ToPage<ATTR, BigDecimal[]> create(
            @NotNull final Class<?> nativeType,
            final int precision,
            final int scale) {
        return new ToBigDecimalFromLongPage<>(nativeType, precision, scale);
    }

    private ToBigDecimalFromLongPage(@NotNull final Class<?> nativeType, final int precision, final int scale) {
        super(nativeType, precision, scale);
    }

    @Override
    @NotNull
    public final Object nullValue() {
        return NULL_LONG_BOXED;
    }

    @Override
    @NotNull
    public final PageMaterializerFactory getPageMaterializerFactory() {
        return new BigDecimalFromLongMaterializer.Factory(scale);
    }
}
