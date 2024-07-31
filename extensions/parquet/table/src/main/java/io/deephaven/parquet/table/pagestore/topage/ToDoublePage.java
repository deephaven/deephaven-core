//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit ToIntPage and run "./gradlew replicateToPage" to regenerate
//
// @formatter:off
package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.parquet.base.materializers.DoubleMaterializer;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE_BOXED;

public class ToDoublePage<ATTR extends Any> implements ToPage<ATTR, double[]> {

    @SuppressWarnings("rawtypes")
    private static final ToDoublePage INSTANCE = new ToDoublePage<>();

    public static <ATTR extends Any> ToDoublePage<ATTR> create(Class<?> nativeType) {
        if (nativeType == null || double.class.equals(nativeType)) {
            // noinspection unchecked
            return INSTANCE;
        }

        throw new IllegalArgumentException("The native type for a Double column is " + nativeType.getCanonicalName());
    }

    private ToDoublePage() {}

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
        return DoubleMaterializer.FACTORY;
    }
}
