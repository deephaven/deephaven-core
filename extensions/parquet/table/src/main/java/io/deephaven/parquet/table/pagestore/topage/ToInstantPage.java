//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.ObjectVectorDirect;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.util.function.LongFunction;
import java.util.function.LongUnaryOperator;

public abstract class ToInstantPage<ATTR extends Any> extends ToLongPage<ATTR> {

    @SuppressWarnings("rawtypes")
    private static final ToInstantPage MILLIS_INSTANCE = new ToInstantPageFromMillis();
    @SuppressWarnings("rawtypes")
    private static final ToInstantPage MICROS_INSTANCE = new ToInstantPageFromMicros();
    @SuppressWarnings("rawtypes")
    private static final ToInstantPage NANOS_INSTANCE = new ToInstantPageFromNanos();

    @SuppressWarnings("unchecked")
    public static <ATTR extends Any> ToPage<ATTR, Instant[]> create(@NotNull final Class<?> nativeType,
            final LogicalTypeAnnotation.TimeUnit unit) {
        if (Instant.class.equals(nativeType)) {
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
                "The native type foran Instant column is " + nativeType.getCanonicalName());
    }

    protected ToInstantPage() {}

    protected static ObjectVector<Instant> makeVectorHelper(final long[] result,
            final LongFunction<Instant> unitToTime) {
        Instant[] to = new Instant[result.length];

        for (int i = 0; i < result.length; ++i) {
            to[i] = unitToTime.apply(result[i]);
        }
        return new ObjectVectorDirect<>(to);
    }

    protected static long[] convertResultHelper(@NotNull final Object result, final LongUnaryOperator unitToNanos) {
        final long[] resultLongs = (long[]) result;
        final int resultLength = resultLongs.length;
        for (int ri = 0; ri < resultLength; ++ri) {
            resultLongs[ri] = unitToNanos.applyAsLong(resultLongs[ri]);
        }
        return resultLongs;
    }

    @Override
    @NotNull
    public final Class<Instant> getNativeComponentType() {
        return Instant.class;
    }

    private static final class ToInstantPageFromNanos<ATTR extends Any> extends ToInstantPage<ATTR> {
        @Override
        @NotNull
        public ObjectVector<Instant> makeVector(long[] result) {
            return makeVectorHelper(result, DateTimeUtils::epochNanosToInstant);
        }
    }

    private static final class ToInstantPageFromMicros<ATTR extends Any> extends ToInstantPage<ATTR> {
        @Override
        @NotNull
        public ObjectVector<Instant> makeVector(long[] result) {
            return makeVectorHelper(result, DateTimeUtils::epochMicrosToInstant);
        }

        @Override
        public long[] convertResult(@NotNull final Object result) {
            return convertResultHelper(result, DateTimeUtils::microsToNanos);
        }
    }

    private static final class ToInstantPageFromMillis<ATTR extends Any> extends ToInstantPage<ATTR> {
        @Override
        @NotNull
        public ObjectVector<Instant> makeVector(long[] result) {
            return makeVectorHelper(result, DateTimeUtils::epochMillisToInstant);
        }

        @Override
        public long[] convertResult(@NotNull final Object result) {
            return convertResultHelper(result, DateTimeUtils::millisToNanos);
        }
    }
}
