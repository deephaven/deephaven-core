/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.parquet.table.util.TransferUtils;
import io.deephaven.util.QueryConstants;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.jetbrains.annotations.NotNull;

import java.time.LocalDateTime;
import java.util.function.LongFunction;

/**
 * Used to convert Parquet TIMESTAMP values with {@code isAdjustedToUTC=false} to {@link LocalDateTime}. Ref: <a href=
 * "https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#local-semantics-timestamps-not-normalized-to-utc">...</a>
 */
public class ToLocalDateTimePage<ATTR extends Any> implements ToPage<ATTR, LocalDateTime[]> {

    @SuppressWarnings("rawtypes")
    private static final ToPage MILLIS_INSTANCE = new ToLocalDateTimePageFromMillis();
    @SuppressWarnings("rawtypes")
    private static final ToPage MICROS_INSTANCE = new ToLocalDateTimePageFromMicros();
    @SuppressWarnings("rawtypes")
    private static final ToPage NANOS_INSTANCE = new ToLocalDateTimePageFromNanos();

    @SuppressWarnings("unchecked")
    public static <ATTR extends Any> ToPage<ATTR, LocalDateTime[]> create(@NotNull final Class<?> nativeType,
            @NotNull final LogicalTypeAnnotation.TimeUnit unit) {
        if (LocalDateTime.class.equals(nativeType)) {
            switch (unit) {
                case MILLIS:
                    return MILLIS_INSTANCE;
                case MICROS:
                    return MICROS_INSTANCE;
                case NANOS:
                    return NANOS_INSTANCE;
                default:
                    throw new IllegalArgumentException("Unsupported unit=" + unit);
            }
        }
        throw new IllegalArgumentException(
                "The native type for a LocalDateTime column is " + nativeType.getCanonicalName());
    }

    ToLocalDateTimePage() {}

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
    public final Object nullValue() {
        return QueryConstants.NULL_LONG_BOXED;
    }

    private static LocalDateTime[] convertResultHelper(@NotNull final Object result,
            @NotNull final LongFunction<LocalDateTime> unitToLocalDateTime) {
        final long[] from = (long[]) result;
        final LocalDateTime[] to = new LocalDateTime[from.length];

        for (int i = 0; i < from.length; ++i) {
            to[i] = unitToLocalDateTime.apply(from[i]);
        }
        return to;
    }

    private static final class ToLocalDateTimePageFromMillis<ATTR extends Any> extends ToLocalDateTimePage<ATTR> {
        @Override
        public LocalDateTime[] convertResult(@NotNull final Object result) {
            return convertResultHelper(result, TransferUtils::epochMillisToLocalDateTimeUTC);
        }
    }

    private static final class ToLocalDateTimePageFromMicros<ATTR extends Any> extends ToLocalDateTimePage<ATTR> {
        @Override
        public LocalDateTime[] convertResult(@NotNull final Object result) {
            return convertResultHelper(result, TransferUtils::epochMicrosToLocalDateTimeUTC);
        }
    }

    private static final class ToLocalDateTimePageFromNanos<ATTR extends Any> extends ToLocalDateTimePage<ATTR> {
        @Override
        public LocalDateTime[] convertResult(@NotNull final Object result) {
            return convertResultHelper(result, TransferUtils::epochNanosToLocalDateTimeUTC);
        }
    }

}
