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
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.time.LocalTime;

public class ToLocalTimePage<ATTR extends Any> implements ToPage<ATTR, LocalTime[]> {

    @SuppressWarnings("unchecked")
    public static <ATTR extends Any> ToPage<ATTR, Instant[]> create(
            final Class<?> nativeType,
            final LogicalTypeAnnotation.TimeUnit unit) {
        if (nativeType == null || LocalTime.class.equals(nativeType)) {
            switch (unit) {
                case MILLIS:
                    return FROM_MILLIS;
                case MICROS:
                    return FROM_MICROS;
                case NANOS:
                    return FROM_NANOS;
                default:
                    throw new IllegalArgumentException("Unsupported unit=" + unit);
            }
        }
        throw new IllegalArgumentException(
                "The native type for a LocalTime column is " + nativeType.getCanonicalName());
    }

    @SuppressWarnings("rawtypes")
    private static final ToPage FROM_MILLIS = new ToLocalTimePage<>(LocalTimeFromMillisMaterializer.Factory);
    @SuppressWarnings("rawtypes")
    private static final ToPage FROM_MICROS = new ToLocalTimePage<>(LocalTimeFromMicrosMaterializer.Factory);
    @SuppressWarnings("rawtypes")
    private static final ToPage FROM_NANOS = new ToLocalTimePage<>(LocalTimeFromNanosMaterializer.Factory);

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
