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
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.time.LocalDateTime;

public class ToLocalDateTimePage<ATTR extends Any> implements ToPage<ATTR, LocalDateTime[]> {

    @SuppressWarnings("unchecked")
    public static <ATTR extends Any> ToPage<ATTR, Instant[]> create(
            final Class<?> nativeType,
            final LogicalTypeAnnotation.TimeUnit unit) {
        if (nativeType == null || LocalDateTime.class.equals(nativeType)) {
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
                "The native type for a LocalDateTime column is " + nativeType.getCanonicalName());
    }

    @SuppressWarnings("rawtypes")
    private static final ToPage FROM_MILLIS = new ToLocalDateTimePage<>(LocalDateTimeFromMillisMaterializer.Factory);
    @SuppressWarnings("rawtypes")
    private static final ToPage FROM_MICROS = new ToLocalDateTimePage<>(LocalDateTimeFromMicrosMaterializer.Factory);
    @SuppressWarnings("rawtypes")
    private static final ToPage FROM_NANOS = new ToLocalDateTimePage<>(LocalDateTimeFromNanosMaterializer.Factory);

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
