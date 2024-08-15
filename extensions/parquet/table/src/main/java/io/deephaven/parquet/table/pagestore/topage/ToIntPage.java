//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.parquet.base.materializers.IntFromBooleanMaterializer;
import io.deephaven.parquet.base.materializers.IntFromUnsignedByteMaterializer;
import io.deephaven.parquet.base.materializers.IntFromUnsignedShortMaterializer;
import io.deephaven.parquet.base.materializers.IntMaterializer;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_INT_BOXED;

public class ToIntPage<ATTR extends Any> implements ToPage<ATTR, int[]> {

    public static <ATTR extends Any> ToIntPage<ATTR> create(Class<?> nativeType) {
        verifyNativeType(nativeType);
        // noinspection unchecked
        return FROM_INT;
    }

    public static <ATTR extends Any> ToIntPage<ATTR> createFromUnsignedShort(final Class<?> nativeType) {
        verifyNativeType(nativeType);
        // noinspection unchecked
        return FROM_UNSIGNED_SHORT;
    }

    public static <ATTR extends Any> ToIntPage<ATTR> createFromUnsignedByte(final Class<?> nativeType) {
        verifyNativeType(nativeType);
        // noinspection unchecked
        return FROM_UNSIGNED_BYTE;
    }

    public static <ARRT extends Any> ToIntPage<ARRT> createFromBoolean(final Class<?> nativeType) {
        verifyNativeType(nativeType);
        // noinspection unchecked
        return FROM_BOOLEAN;
    }

    @SuppressWarnings("rawtypes")
    private static final ToIntPage FROM_INT = new ToIntPage<>(IntMaterializer.FACTORY);
    @SuppressWarnings("rawtypes")
    private static final ToIntPage FROM_UNSIGNED_SHORT = new ToIntPage<>(IntFromUnsignedShortMaterializer.FACTORY);
    @SuppressWarnings("rawtypes")
    private static final ToIntPage FROM_UNSIGNED_BYTE = new ToIntPage<>(IntFromUnsignedByteMaterializer.FACTORY);
    @SuppressWarnings("rawtypes")
    private static final ToIntPage FROM_BOOLEAN = new ToIntPage<>(IntFromBooleanMaterializer.FACTORY);

    private static void verifyNativeType(final Class<?> nativeType) {
        if (nativeType == null || int.class.equals(nativeType)) {
            return;
        }
        throw new IllegalArgumentException("The native type for a Int column is " + nativeType.getCanonicalName());
    }

    private final PageMaterializerFactory pageMaterializerFactory;

    private ToIntPage(@NotNull final PageMaterializerFactory pageMaterializerFactory) {
        this.pageMaterializerFactory = pageMaterializerFactory;
    }

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
        return pageMaterializerFactory;
    }
}
