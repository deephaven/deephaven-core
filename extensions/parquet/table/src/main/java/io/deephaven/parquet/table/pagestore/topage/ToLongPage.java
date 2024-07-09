//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.parquet.base.materializers.LongFromUnsignedIntMaterializer;
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

    @SuppressWarnings("rawtypes")
    private static final ToLongPage FROM_LONG = new ToLongPage<>(LongMaterializer.FACTORY);
    @SuppressWarnings("rawtypes")
    private static final ToLongPage FROM_UNSIGNED_INT = new ToLongPage<>(LongFromUnsignedIntMaterializer.FACTORY);

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
