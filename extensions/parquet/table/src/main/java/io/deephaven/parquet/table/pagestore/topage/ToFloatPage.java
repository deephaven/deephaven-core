//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.parquet.base.materializers.FloatFromFloat16Materializer;
import io.deephaven.parquet.base.materializers.FloatMaterializer;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_FLOAT_BOXED;

public class ToFloatPage<ATTR extends Any> implements ToPage<ATTR, float[]> {

    public static <ATTR extends Any> ToFloatPage<ATTR> create(final Class<?> nativeType) {
        verifyNativeType(nativeType);
        // noinspection unchecked
        return FROM_FLOAT;
    }

    public static <ATTR extends Any> ToFloatPage<ATTR> createFromFloat16(final Class<?> nativeType) {
        verifyNativeType(nativeType);
        // noinspection unchecked
        return FROM_FLOAT16;
    }

    @SuppressWarnings("rawtypes")
    private static final ToFloatPage FROM_FLOAT = new ToFloatPage<>(FloatMaterializer.FACTORY);
    @SuppressWarnings("rawtypes")
    private static final ToFloatPage FROM_FLOAT16 = new ToFloatPage<>(FloatFromFloat16Materializer.FACTORY);

    private static void verifyNativeType(final Class<?> nativeType) {
        if (nativeType == null || float.class.equals(nativeType)) {
            return;
        }
        throw new IllegalArgumentException("The native type for a Float column is " + nativeType.getCanonicalName());
    }

    private final PageMaterializerFactory pageMaterializerFactory;

    private ToFloatPage(@NotNull final PageMaterializerFactory pageMaterializerFactory) {
        this.pageMaterializerFactory = pageMaterializerFactory;
    }

    @Override
    @NotNull
    public final Class<Float> getNativeType() {
        return float.class;
    }

    @Override
    @NotNull
    public final ChunkType getChunkType() {
        return ChunkType.Float;
    }

    @Override
    @NotNull
    public final Object nullValue() {
        return NULL_FLOAT_BOXED;
    }

    @Override
    @NotNull
    public final PageMaterializerFactory getPageMaterializerFactory() {
        return pageMaterializerFactory;
    }
}
