package io.deephaven.engine.v2.locations.parquet.topage;

import io.deephaven.engine.tables.dbarrays.ObjectVector;
import io.deephaven.engine.tables.dbarrays.ObjectVectorDirect;
import io.deephaven.engine.tables.utils.DateTime;
import io.deephaven.engine.tables.utils.DateTimeUtils;
import io.deephaven.engine.v2.sources.chunk.Attributes;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.jetbrains.annotations.NotNull;

import java.util.function.LongFunction;
import java.util.function.LongUnaryOperator;

public abstract class ToDateTimePage<ATTR extends Attributes.Any> extends ToLongPage<ATTR> {

    @SuppressWarnings("rawtypes")
    private static final ToDateTimePage MILLIS_INSTANCE = new ToDateTimePageFromMillis();
    @SuppressWarnings("rawtypes")
    private static final ToDateTimePage MICROS_INSTANCE = new ToDateTimePageFromMicros();
    @SuppressWarnings("rawtypes")
    private static final ToDateTimePage NANOS_INSTANCE = new ToDateTimePageFromNanos();

    @SuppressWarnings("unchecked")
    public static <ATTR extends Attributes.Any> ToPage<ATTR, DateTime[]> create(@NotNull final Class<?> nativeType,
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

    protected static ObjectVector<DateTime> makeDbArrayHelper(final long[] result,
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

    private static final class ToDateTimePageFromNanos<ATTR extends Attributes.Any> extends ToDateTimePage<ATTR> {
        @Override
        @NotNull
        public ObjectVector<DateTime> makeDbArray(long[] result) {
            return makeDbArrayHelper(result, DateTimeUtils::nanosToTime);
        }
    }

    private static final class ToDateTimePageFromMicros<ATTR extends Attributes.Any> extends ToDateTimePage<ATTR> {
        @Override
        @NotNull
        public ObjectVector<DateTime> makeDbArray(long[] result) {
            return makeDbArrayHelper(result, DateTimeUtils::microsToTime);
        }

        @Override
        public final long[] convertResult(@NotNull final Object result) {
            return convertResultHelper(result, DateTimeUtils::microsToNanos);
        }
    }

    private static final class ToDateTimePageFromMillis<ATTR extends Attributes.Any> extends ToDateTimePage<ATTR> {
        @Override
        @NotNull
        public ObjectVector<DateTime> makeDbArray(long[] result) {
            return makeDbArrayHelper(result, DateTimeUtils::millisToTime);
        }

        @Override
        public final long[] convertResult(@NotNull final Object result) {
            return convertResultHelper(result, DateTimeUtils::millisToNanos);
        }
    }
}
