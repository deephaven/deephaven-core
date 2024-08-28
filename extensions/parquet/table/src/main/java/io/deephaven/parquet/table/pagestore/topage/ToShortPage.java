//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.parquet.base.materializers.ShortFromBooleanMaterializer;
import io.deephaven.parquet.base.materializers.ShortFromUnsignedByteMaterializer;
import io.deephaven.parquet.base.materializers.ShortMaterializer;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_SHORT_BOXED;

public class ToShortPage<ATTR extends Any> implements ToPage<ATTR, short[]> {

    public static <ATTR extends Any> ToShortPage<ATTR> create(final Class<?> nativeType) {
        verifyNativeType(nativeType);
        // noinspection unchecked
        return FROM_SHORT;
    }

    public static <ATTR extends Any> ToShortPage<ATTR> createFromUnsignedByte(final Class<?> nativeType) {
        verifyNativeType(nativeType);
        // noinspection unchecked
        return FROM_UNSIGNED_BYTE;
    }

    public static <ATTR extends Any> ToShortPage<ATTR> createFromBoolean(final Class<?> nativeType) {
        verifyNativeType(nativeType);
        // noinspection unchecked
        return FROM_BOOLEAN;
    }

    @SuppressWarnings("rawtypes")
    private static final ToShortPage FROM_SHORT = new ToShortPage<>(ShortMaterializer.FACTORY);
    @SuppressWarnings("rawtypes")
    private static final ToShortPage FROM_UNSIGNED_BYTE = new ToShortPage<>(ShortFromUnsignedByteMaterializer.FACTORY);
    @SuppressWarnings("rawtypes")
    private static final ToShortPage FROM_BOOLEAN = new ToShortPage<>(ShortFromBooleanMaterializer.FACTORY);

    private static void verifyNativeType(final Class<?> nativeType) {
        if (nativeType == null || short.class.equals(nativeType)) {
            return;
        }
        throw new IllegalArgumentException("The native type for a Short column is " + nativeType.getCanonicalName());
    }

    private final PageMaterializerFactory pageMaterializerFactory;

    private ToShortPage(@NotNull final PageMaterializerFactory pageMaterializerFactory) {
        this.pageMaterializerFactory = pageMaterializerFactory;
    }

    @Override
    @NotNull
    public final Class<Short> getNativeType() {
        return short.class;
    }

    @Override
    @NotNull
    public final ChunkType getChunkType() {
        return ChunkType.Short;
    }

    @Override
    @NotNull
    public final Object nullValue() {
        return NULL_SHORT_BOXED;
    }

    @Override
    @NotNull
    public final PageMaterializerFactory getPageMaterializerFactory() {
        return pageMaterializerFactory;
    }
}
