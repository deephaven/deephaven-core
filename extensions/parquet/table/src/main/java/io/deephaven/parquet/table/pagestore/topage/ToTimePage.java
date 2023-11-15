/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.jetbrains.annotations.NotNull;

import java.time.LocalTime;
import java.util.function.LongFunction;

public class ToTimePage<ATTR extends Any> implements ToPage<ATTR, LocalTime[]> {

    @SuppressWarnings("rawtypes")
    private static final ToPage MILLIS_INSTANCE = new ToTimePageFromMillis();
    @SuppressWarnings("rawtypes")
    private static final ToPage MICROS_INSTANCE = new ToTimePageFromMicros();
    @SuppressWarnings("rawtypes")
    private static final ToPage NANOS_INSTANCE = new ToTimePageFromNanos();

    @SuppressWarnings("unchecked")
    public static <ATTR extends Any> ToPage<ATTR, LocalTime[]> create(
            @NotNull final Class<?> nativeType,
            @NotNull final LogicalTypeAnnotation.TimeUnit unit,
            @SuppressWarnings("unused") final boolean isAdjustedToUTC) {
        // isAdjustedToUTC parameter is ignored while reading from Parquet files
        if (LocalTime.class.equals(nativeType)) {
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
        throw new IllegalArgumentException("The native type for a Time column is " + nativeType.getCanonicalName());
    }

    ToTimePage() {}

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

    private static final class ToTimePageFromMillis<ATTR extends Any> extends ToTimePage<ATTR> {
        @Override
        @NotNull
        public Object nullValue() {
            return QueryConstants.NULL_INT_BOXED;
        }

        @Override
        public LocalTime[] convertResult(@NotNull final Object result) {
            final int[] from = (int[]) result;
            final LocalTime[] to = new LocalTime[from.length];

            for (int i = 0; i < from.length; ++i) {
                to[i] = DateTimeUtils.millisOfDayToLocalTime(from[i]);
            }
            return to;
        }
    }

    private static class ToTimePageFromLong<ATTR extends Any> extends ToTimePage<ATTR> {
        @Override
        @NotNull
        public final Object nullValue() {
            return QueryConstants.NULL_LONG_BOXED;
        }

        static LocalTime[] convertResultHelper(@NotNull final Object result,
                @NotNull final LongFunction<LocalTime> unitToLocalTime) {
            final long[] from = (long[]) result;
            final LocalTime[] to = new LocalTime[from.length];

            for (int i = 0; i < from.length; ++i) {
                to[i] = unitToLocalTime.apply(from[i]);
            }
            return to;
        }
    }

    private static final class ToTimePageFromMicros<ATTR extends Any> extends ToTimePageFromLong<ATTR> {
        @Override
        public LocalTime[] convertResult(@NotNull final Object result) {
            return convertResultHelper(result, DateTimeUtils::microsOfDayToLocalTime);
        }
    }

    private static final class ToTimePageFromNanos<ATTR extends Any> extends ToTimePageFromLong<ATTR> {
        @Override
        public LocalTime[] convertResult(@NotNull final Object result) {
            return convertResultHelper(result, DateTimeUtils::nanosOfDayToLocalTime);
        }
    }
}
