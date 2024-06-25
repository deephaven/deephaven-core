//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.parquet.base.materializers.LocalDateTimeFromMicrosMaterializer;
import io.deephaven.parquet.base.materializers.LocalDateTimeFromMillisMaterializer;
import io.deephaven.parquet.base.materializers.LocalDateTimeFromNanosMaterializer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.time.LocalDateTime;

public abstract class ToLocalDateTimePage<ATTR extends Any> implements ToPage<ATTR, LocalDateTime[]> {

    @SuppressWarnings("rawtypes")
    private static final ToPage FROM_MILLIS_INSTANCE = new FromMillis();
    @SuppressWarnings("rawtypes")
    private static final ToPage FROM_MICROS_INSTANCE = new FromMicros();
    @SuppressWarnings("rawtypes")
    private static final ToPage FROM_NANOS_INSTANCE = new FromNanos();

    @SuppressWarnings("unchecked")
    public static <ATTR extends Any> ToPage<ATTR, Instant[]> create(
            final Class<?> nativeType,
            final LogicalTypeAnnotation.TimeUnit unit) {
        if (nativeType == null || LocalDateTime.class.equals(nativeType)) {
            switch (unit) {
                case MILLIS:
                    return FROM_MILLIS_INSTANCE;
                case MICROS:
                    return FROM_MICROS_INSTANCE;
                case NANOS:
                    return FROM_NANOS_INSTANCE;
                default:
                    throw new IllegalArgumentException("Unsupported unit=" + unit);
            }
        }
        throw new IllegalArgumentException(
                "The native type for a LocalDateTime column is " + nativeType.getCanonicalName());
    }

    private static final class FromMillis<ATTR extends Any> extends ToLocalDateTimePage<ATTR> {
        @Override
        @NotNull
        public PageMaterializerFactory getPageMaterializerFactory() {
            return LocalDateTimeFromMillisMaterializer.Factory;
        }
    }

    private static final class FromMicros<ATTR extends Any> extends ToLocalDateTimePage<ATTR> {
        @Override
        @NotNull
        public PageMaterializerFactory getPageMaterializerFactory() {
            return LocalDateTimeFromMicrosMaterializer.Factory;
        }
    }

    private static final class FromNanos<ATTR extends Any> extends ToLocalDateTimePage<ATTR> {
        @Override
        @NotNull
        public PageMaterializerFactory getPageMaterializerFactory() {
            return LocalDateTimeFromNanosMaterializer.Factory;
        }
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
}
