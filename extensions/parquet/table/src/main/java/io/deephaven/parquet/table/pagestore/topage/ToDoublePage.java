//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.parquet.base.materializers.DoubleFromFloatMaterializer;
import io.deephaven.parquet.base.materializers.DoubleMaterializer;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE_BOXED;

public class ToDoublePage<ATTR extends Any> implements ToPage<ATTR, double[]> {

    public static <ATTR extends Any> ToDoublePage<ATTR> create(final Class<?> nativeType) {
        verifyNativeType(nativeType);
        // noinspection unchecked
        return FROM_DOUBLE;
    }

    public static <ATTR extends Any> ToDoublePage<ATTR> createFromFloat(final Class<?> nativeType) {
        verifyNativeType(nativeType);
        // noinspection unchecked
        return FROM_FLOAT;
    }

    @SuppressWarnings("rawtypes")
    private static final ToDoublePage FROM_DOUBLE = new ToDoublePage<>(DoubleMaterializer.FACTORY);
    @SuppressWarnings("rawtypes")
    private static final ToDoublePage FROM_FLOAT = new ToDoublePage<>(DoubleFromFloatMaterializer.FACTORY);

    private static void verifyNativeType(final Class<?> nativeType) {
        if (nativeType == null || double.class.equals(nativeType)) {
            return;
        }
        throw new IllegalArgumentException("The native type for a Double column is " + nativeType.getCanonicalName());
    }

    private final PageMaterializerFactory pageMaterializerFactory;

    private ToDoublePage(@NotNull final PageMaterializerFactory pageMaterializerFactory) {
        this.pageMaterializerFactory = pageMaterializerFactory;
    }

    @Override
    @NotNull
    public final Class<Double> getNativeType() {
        return double.class;
    }

    @Override
    @NotNull
    public final ChunkType getChunkType() {
        return ChunkType.Double;
    }

    @Override
    @NotNull
    public final Object nullValue() {
        return NULL_DOUBLE_BOXED;
    }

    @Override
    @NotNull
    public final PageMaterializerFactory getPageMaterializerFactory() {
        return pageMaterializerFactory;
    }
}
