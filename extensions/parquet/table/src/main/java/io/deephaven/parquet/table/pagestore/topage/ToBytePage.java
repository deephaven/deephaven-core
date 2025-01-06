//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.parquet.base.materializers.BooleanAsByteMaterializer;
import io.deephaven.parquet.base.materializers.ByteMaterializer;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_BYTE_BOXED;

public class ToBytePage<ATTR extends Any> implements ToPage<ATTR, byte[]> {

    public static <ATTR extends Any> ToBytePage<ATTR> create(Class<?> nativeType) {
        verifyNativeType(nativeType);
        // noinspection unchecked
        return FROM_BYTE;
    }

    public static <ATTR extends Any> ToBytePage<ATTR> createFromBoolean(final Class<?> nativeType) {
        verifyNativeType(nativeType);
        // noinspection unchecked
        return FROM_BOOLEAN;
    }

    @SuppressWarnings("rawtypes")
    private static final ToBytePage FROM_BYTE = new ToBytePage<>(ByteMaterializer.FACTORY);
    @SuppressWarnings("rawtypes")
    private static final ToBytePage FROM_BOOLEAN = new ToBytePage<>(BooleanAsByteMaterializer.FACTORY);

    private static void verifyNativeType(final Class<?> nativeType) {
        if (nativeType == null || byte.class.equals(nativeType)) {
            return;
        }
        throw new IllegalArgumentException("The native type for a Byte column is " + nativeType.getCanonicalName());
    }

    private final PageMaterializerFactory pageMaterializerFactory;

    private ToBytePage(@NotNull final PageMaterializerFactory pageMaterializerFactory) {
        this.pageMaterializerFactory = pageMaterializerFactory;
    }

    @Override
    @NotNull
    public final Class<Byte> getNativeType() {
        return byte.class;
    }

    @Override
    @NotNull
    public final ChunkType getChunkType() {
        return ChunkType.Byte;
    }

    @Override
    @NotNull
    public final Object nullValue() {
        return NULL_BYTE_BOXED;
    }

    @Override
    @NotNull
    public final PageMaterializerFactory getPageMaterializerFactory() {
        return pageMaterializerFactory;
    }
}
