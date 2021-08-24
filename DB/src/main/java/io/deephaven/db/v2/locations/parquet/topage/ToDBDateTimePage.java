package io.deephaven.db.v2.locations.parquet.topage;

import io.deephaven.db.tables.dbarrays.DbArray;
import io.deephaven.db.tables.dbarrays.DbArrayDirect;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.v2.sources.chunk.Attributes;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.jetbrains.annotations.NotNull;

import java.util.function.LongFunction;
import java.util.function.LongUnaryOperator;

public abstract class ToDBDateTimePage<ATTR extends Attributes.Any> extends ToLongPage<ATTR> {

    @SuppressWarnings("rawtypes")
    private static final ToDBDateTimePage MILLIS_INSTANCE = new ToDBDateTimePageFromMillis();
    @SuppressWarnings("rawtypes")
    private static final ToDBDateTimePage MICROS_INSTANCE = new ToDBDateTimePageFromMicros();
    @SuppressWarnings("rawtypes")
    private static final ToDBDateTimePage NANOS_INSTANCE = new ToDBDateTimePageFromNanos();

    @SuppressWarnings("unchecked")
    public static <ATTR extends Attributes.Any> ToPage<ATTR, DBDateTime[]> create(
        @NotNull final Class<?> nativeType, final LogicalTypeAnnotation.TimeUnit unit) {
        if (DBDateTime.class.equals(nativeType)) {
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
            "The native type for a DBDateTime column is " + nativeType.getCanonicalName());
    }

    protected ToDBDateTimePage() {}

    protected static DbArray<DBDateTime> makeDbArrayHelper(final long[] result,
        final LongFunction<DBDateTime> unitToTime) {
        DBDateTime[] to = new DBDateTime[result.length];

        for (int i = 0; i < result.length; ++i) {
            to[i] = unitToTime.apply(result[i]);
        }
        return new DbArrayDirect<>(to);
    }

    protected static long[] convertResultHelper(@NotNull final Object result,
        final LongUnaryOperator unitToNanos) {
        final long[] resultLongs = (long[]) result;
        final int resultLength = resultLongs.length;
        for (int ri = 0; ri < resultLength; ++ri) {
            resultLongs[ri] = unitToNanos.applyAsLong(resultLongs[ri]);
        }
        return resultLongs;
    }

    @Override
    @NotNull
    public final Class<DBDateTime> getNativeComponentType() {
        return DBDateTime.class;
    }

    private static final class ToDBDateTimePageFromNanos<ATTR extends Attributes.Any>
        extends ToDBDateTimePage<ATTR> {
        @Override
        @NotNull
        public DbArray<DBDateTime> makeDbArray(long[] result) {
            return makeDbArrayHelper(result, DBTimeUtils::nanosToTime);
        }
    }

    private static final class ToDBDateTimePageFromMicros<ATTR extends Attributes.Any>
        extends ToDBDateTimePage<ATTR> {
        @Override
        @NotNull
        public DbArray<DBDateTime> makeDbArray(long[] result) {
            return makeDbArrayHelper(result, DBTimeUtils::microsToTime);
        }

        @Override
        public final long[] convertResult(@NotNull final Object result) {
            return convertResultHelper(result, DBTimeUtils::microsToNanos);
        }
    }

    private static final class ToDBDateTimePageFromMillis<ATTR extends Attributes.Any>
        extends ToDBDateTimePage<ATTR> {
        @Override
        @NotNull
        public DbArray<DBDateTime> makeDbArray(long[] result) {
            return makeDbArrayHelper(result, DBTimeUtils::millisToTime);
        }

        @Override
        public final long[] convertResult(@NotNull final Object result) {
            return convertResultHelper(result, DBTimeUtils::millisToNanos);
        }
    }
}
