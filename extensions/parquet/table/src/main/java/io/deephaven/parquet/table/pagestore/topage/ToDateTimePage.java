package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.ObjectVectorDirect;
import io.deephaven.time.DateTime;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.jetbrains.annotations.NotNull;

import java.util.function.LongFunction;
import java.util.function.LongUnaryOperator;

public abstract class ToDateTimePage<ATTR extends Any> extends ToLongPage<ATTR> {

    @SuppressWarnings("rawtypes")
    private static final ToDateTimePage MILLIS_INSTANCE = new ToDateTimePageFromMillis();
    @SuppressWarnings("rawtypes")
    private static final ToDateTimePage MICROS_INSTANCE = new ToDateTimePageFromMicros();
    @SuppressWarnings("rawtypes")
    private static final ToDateTimePage NANOS_INSTANCE = new ToDateTimePageFromNanos();

    @SuppressWarnings("unchecked")
    public static <ATTR extends Any> ToPage<ATTR, DateTime[]> create(@NotNull final Class<?> nativeType,
            final LogicalTypeAnnotation.TimeUnit unit) {
        if (DateTime.class.equals(nativeType)) {
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
                "The native type for a DateTime column is " + nativeType.getCanonicalName());
    }

    protected ToDateTimePage() {}

    protected static ObjectVector<DateTime> makeVectorHelper(final long[] result,
            final LongFunction<DateTime> unitToTime) {
        DateTime[] to = new DateTime[result.length];

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
    public final Class<DateTime> getNativeComponentType() {
        return DateTime.class;
    }

    private static final class ToDateTimePageFromNanos<ATTR extends Any> extends ToDateTimePage<ATTR> {
        @Override
        @NotNull
        public ObjectVector<DateTime> makeVector(long[] result) {
            return makeVectorHelper(result, DateTimeUtils::nanosToTime);
        }
    }

    private static final class ToDateTimePageFromMicros<ATTR extends Any> extends ToDateTimePage<ATTR> {
        @Override
        @NotNull
        public ObjectVector<DateTime> makeVector(long[] result) {
            return makeVectorHelper(result, DateTimeUtils::microsToTime);
        }

        @Override
        public long[] convertResult(@NotNull final Object result) {
            return convertResultHelper(result, DateTimeUtils::microsToNanos);
        }
    }

    private static final class ToDateTimePageFromMillis<ATTR extends Any> extends ToDateTimePage<ATTR> {
        @Override
        @NotNull
        public ObjectVector<DateTime> makeVector(long[] result) {
            return makeVectorHelper(result, DateTimeUtils::millisToTime);
        }

        @Override
        public long[] convertResult(@NotNull final Object result) {
            return convertResultHelper(result, DateTimeUtils::millisToNanos);
        }
    }
}
