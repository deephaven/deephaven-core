//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.parquet.base.materializers.LongFromBooleanMaterializer;
import io.deephaven.parquet.base.materializers.LongFromIntMaterializer;
import io.deephaven.parquet.base.materializers.LongFromUnsignedByteMaterializer;
import io.deephaven.parquet.base.materializers.LongFromUnsignedIntMaterializer;
import io.deephaven.parquet.base.materializers.LongFromUnsignedShortMaterializer;
import io.deephaven.parquet.base.materializers.LongMaterializer;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_LONG_BOXED;

public class ToLongPage<ATTR extends Any> implements ToPage<ATTR, long[]> {

    public static <ATTR extends Any> ToLongPage<ATTR> create(final Class<?> nativeType) {
        verifyNativeType(nativeType);
        // noinspection unchecked
        return FROM_LONG;
    }

    public static <ATTR extends Any> ToLongPage<ATTR> createFromUnsignedInt(final Class<?> nativeType) {
        verifyNativeType(nativeType);
        // noinspection unchecked
        return FROM_UNSIGNED_INT;
    }

    public static <ATTR extends Any> ToLongPage<ATTR> createFromInt(final Class<?> nativeType) {
        verifyNativeType(nativeType);
        // noinspection unchecked
        return FROM_INT;
    }

    public static <ATTR extends Any> ToLongPage<ATTR> createFromUnsignedShort(final Class<?> nativeType) {
        verifyNativeType(nativeType);
        // noinspection unchecked
        return FROM_UNSIGNED_SHORT;
    }

    public static <ATTR extends Any> ToLongPage<ATTR> createFromUnsignedByte(final Class<?> nativeType) {
        verifyNativeType(nativeType);
        // noinspection unchecked
        return FROM_UNSIGNED_BYTE;
    }

    public static <ATTR extends Any> ToLongPage<ATTR> createFromBoolean(final Class<?> nativeType) {
        verifyNativeType(nativeType);
        // noinspection unchecked
        return FROM_BOOLEAN;
    }

    @SuppressWarnings("rawtypes")
    private static final ToLongPage FROM_LONG = new ToLongPage<>(LongMaterializer.FACTORY);
    @SuppressWarnings("rawtypes")
    private static final ToLongPage FROM_UNSIGNED_INT = new ToLongPage<>(LongFromUnsignedIntMaterializer.FACTORY);
    @SuppressWarnings("rawtypes")
    private static final ToLongPage FROM_INT = new ToLongPage<>(LongFromIntMaterializer.FACTORY);
    @SuppressWarnings("rawtypes")
    private static final ToLongPage FROM_UNSIGNED_SHORT = new ToLongPage<>(LongFromUnsignedShortMaterializer.FACTORY);
    @SuppressWarnings("rawtypes")
    private static final ToLongPage FROM_UNSIGNED_BYTE = new ToLongPage<>(LongFromUnsignedByteMaterializer.FACTORY);
    @SuppressWarnings("rawtypes")
    private static final ToLongPage FROM_BOOLEAN = new ToLongPage<>(LongFromBooleanMaterializer.FACTORY);

    private static void verifyNativeType(final Class<?> nativeType) {
        if (nativeType == null || long.class.equals(nativeType)) {
            return;
        }
        throw new IllegalArgumentException("The native type for a Long column is " + nativeType.getCanonicalName());
    }

    private final PageMaterializerFactory pageMaterializerFactory;

    private ToLongPage(@NotNull final PageMaterializerFactory pageMaterializerFactory) {
        this.pageMaterializerFactory = pageMaterializerFactory;
    }

    @Override
    @NotNull
    public final Class<Long> getNativeType() {
        return long.class;
    }

    @Override
    @NotNull
    public final ChunkType getChunkType() {
        return ChunkType.Long;
    }

    @Override
    @NotNull
    public final Object nullValue() {
        return NULL_LONG_BOXED;
    }

    @Override
    @NotNull
    public final PageMaterializerFactory getPageMaterializerFactory() {
        return pageMaterializerFactory;
    }
}
