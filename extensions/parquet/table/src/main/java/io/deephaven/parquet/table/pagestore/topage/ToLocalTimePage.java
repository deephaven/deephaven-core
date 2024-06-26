//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit ToLocalDateTimePage and run "./gradlew replicateToPage" to regenerate
//
// @formatter:off
package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.parquet.base.materializers.LocalTimeFromMillisMaterializer;
import io.deephaven.parquet.base.materializers.LocalTimeFromMicrosMaterializer;
import io.deephaven.parquet.base.materializers.LocalTimeFromNanosMaterializer;
import org.jetbrains.annotations.NotNull;

import java.time.LocalTime;

public class ToLocalTimePage<ATTR extends Any> implements ToPage<ATTR, LocalTime[]> {

    public static <ATTR extends Any> ToPage<ATTR, LocalTime[]> createFromMillis(final Class<?> nativeType) {
        verifyNativeType(nativeType);
        // noinspection unchecked
        return FROM_MILLIS;
    }

    public static <ATTR extends Any> ToPage<ATTR, LocalTime[]> createFromMicros(final Class<?> nativeType) {
        verifyNativeType(nativeType);
        // noinspection unchecked
        return FROM_MICROS;
    }

    public static <ATTR extends Any> ToPage<ATTR, LocalTime[]> createFromNanos(final Class<?> nativeType) {
        verifyNativeType(nativeType);
        // noinspection unchecked
        return FROM_NANOS;
    }

    @SuppressWarnings("rawtypes")
    private static final ToPage FROM_MILLIS = new ToLocalTimePage<>(LocalTimeFromMillisMaterializer.FACTORY);
    @SuppressWarnings("rawtypes")
    private static final ToPage FROM_MICROS = new ToLocalTimePage<>(LocalTimeFromMicrosMaterializer.FACTORY);
    @SuppressWarnings("rawtypes")
    private static final ToPage FROM_NANOS = new ToLocalTimePage<>(LocalTimeFromNanosMaterializer.FACTORY);

    private static void verifyNativeType(final Class<?> nativeType) {
        if (nativeType != null && !LocalTime.class.equals(nativeType)) {
            throw new IllegalArgumentException(
                    "The native type for a LocalTime column is " + nativeType.getCanonicalName());
        }
    }

    private final PageMaterializerFactory pageMaterializerFactory;

    private ToLocalTimePage(@NotNull final PageMaterializerFactory pageMaterializerFactory) {
        this.pageMaterializerFactory = pageMaterializerFactory;
    }

    @Override
    @NotNull
    public final Class<LocalTime> getNativeType() {
        return LocalTime.class;
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
