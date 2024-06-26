//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.parquet.base.materializers.LocalDateTimeFromMillisMaterializer;
import io.deephaven.parquet.base.materializers.LocalDateTimeFromMicrosMaterializer;
import io.deephaven.parquet.base.materializers.LocalDateTimeFromNanosMaterializer;
import org.jetbrains.annotations.NotNull;

import java.time.LocalDateTime;

public class ToLocalDateTimePage<ATTR extends Any> implements ToPage<ATTR, LocalDateTime[]> {

    public static <ATTR extends Any> ToPage<ATTR, LocalDateTime[]> createFromMillis(final Class<?> nativeType) {
        verifyNativeType(nativeType);
        // noinspection unchecked
        return FROM_MILLIS;
    }

    public static <ATTR extends Any> ToPage<ATTR, LocalDateTime[]> createFromMicros(final Class<?> nativeType) {
        verifyNativeType(nativeType);
        // noinspection unchecked
        return FROM_MICROS;
    }

    public static <ATTR extends Any> ToPage<ATTR, LocalDateTime[]> createFromNanos(final Class<?> nativeType) {
        verifyNativeType(nativeType);
        // noinspection unchecked
        return FROM_NANOS;
    }

    @SuppressWarnings("rawtypes")
    private static final ToPage FROM_MILLIS = new ToLocalDateTimePage<>(LocalDateTimeFromMillisMaterializer.FACTORY);
    @SuppressWarnings("rawtypes")
    private static final ToPage FROM_MICROS = new ToLocalDateTimePage<>(LocalDateTimeFromMicrosMaterializer.FACTORY);
    @SuppressWarnings("rawtypes")
    private static final ToPage FROM_NANOS = new ToLocalDateTimePage<>(LocalDateTimeFromNanosMaterializer.FACTORY);

    private static void verifyNativeType(final Class<?> nativeType) {
        if (nativeType != null && !LocalDateTime.class.equals(nativeType)) {
            throw new IllegalArgumentException(
                    "The native type for a LocalDateTime column is " + nativeType.getCanonicalName());
        }
    }

    private final PageMaterializerFactory pageMaterializerFactory;

    private ToLocalDateTimePage(@NotNull final PageMaterializerFactory pageMaterializerFactory) {
        this.pageMaterializerFactory = pageMaterializerFactory;
    }

    @Override
    @NotNull
    public final Class<LocalDateTime> getNativeType() {
        return LocalDateTime.class;
    }

    @Override
    @NotNull
    public final ChunkType getChunkType() {
        return ChunkType.Object;
    }

    @Override
    @NotNull
    public final PageMaterializerFactory getPageMaterializerFactory() {
        return pageMaterializerFactory;
    }
}
