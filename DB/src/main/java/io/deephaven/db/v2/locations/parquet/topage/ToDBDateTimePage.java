package io.deephaven.db.v2.locations.parquet.topage;

import io.deephaven.db.tables.dbarrays.DbArray;
import io.deephaven.db.tables.dbarrays.DbArrayDirect;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.v2.sources.chunk.Attributes;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.jetbrains.annotations.NotNull;

import java.util.function.LongUnaryOperator;

public abstract class ToDBDateTimePage<ATTR extends Attributes.Any> extends ToLongPage<ATTR> {

    private static <ATTR extends Attributes.Any> ToDBDateTimePage<ATTR> fromUnitToNanos(final LongUnaryOperator convertUnitToNanos) {
        return new ToDBDateTimePage<ATTR>() {
            private long toNanos(final long v) {
                return convertUnitToNanos.applyAsLong(v);
            }

            @Override
            public final long[] convertResult(@NotNull final Object result) {
                final long[] resultLongs = (long[]) result;

                final int resultLength = resultLongs.length;
                for (int ri = 0; ri < resultLength; ++ri) {
                    resultLongs[ri] = toNanos(resultLongs[ri]);
                }

                return resultLongs;
            }

            @Override
            @NotNull
            public DbArray<DBDateTime> makeDbArray(long[] result) {
                DBDateTime[] to = new DBDateTime[result.length];

                for (int i = 0; i < result.length; ++i) {
                    to[i] = new DBDateTime(toNanos(result[i]));
                }

                return new DbArrayDirect<>(to);
            }
        };
    }

    private static long longIdentity(final long v) {
        return v;
    }

    private static final ToDBDateTimePage<?> MILLIS_INSTANCE = fromUnitToNanos(DBTimeUtils::millisToNanos);
    private static final ToDBDateTimePage<?> MICROS_INSTANCE = fromUnitToNanos(DBTimeUtils::microsToNanos);
    private static final ToDBDateTimePage<?> NANOS_INSTANCE = fromUnitToNanos(ToDBDateTimePage::longIdentity);

    public static ToDBDateTimePage create(@NotNull final Class<?> nativeType, final LogicalTypeAnnotation.TimeUnit unit) {
        if (DBDateTime.class.equals(nativeType)) {
            switch(unit) {
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

        throw new IllegalArgumentException("The native type for a DBDateTime column is " + nativeType.getCanonicalName());
    }

    protected ToDBDateTimePage() {}

    @Override
    @NotNull
    public final Class<DBDateTime> getNativeComponentType() {
        return DBDateTime.class;
    }
}
