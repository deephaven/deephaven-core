package io.deephaven.db.v2.locations.parquet.topage;

import io.deephaven.db.tables.dbarrays.DbArray;
import io.deephaven.db.tables.dbarrays.DbArrayDirect;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.v2.sources.chunk.Attributes;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.jetbrains.annotations.NotNull;

public abstract class ToDBDateTimePage<ATTR extends Attributes.Any> extends ToLongPage<ATTR> {

    private static final ToDBDateTimePage MILLIS_INSTANCE = new ToDBDateTimePageMillis<>();
    private static final ToDBDateTimePage MICROS_INSTANCE = new ToDBDateTimePageMicros<>();
    private static final ToDBDateTimePage NANOS_INSTANCE = new ToDBDateTimePageNanos<>();

    public static <ATTR extends Attributes.Any> ToDBDateTimePage<ATTR> create(@NotNull final Class<?> nativeType, final LogicalTypeAnnotation.TimeUnit unit) {
        if (DBDateTime.class.equals(nativeType)) {
            switch(unit) {
                case MILLIS:
                    //noinspection unchecked
                    return MILLIS_INSTANCE;
                case MICROS:
                    //noinspection unchecked
                    return MICROS_INSTANCE;
                case NANOS:
                    //noinspection unchecked
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

    protected abstract DBDateTime toDBDateTime(final long v);

    @Override
    @NotNull
    public DbArray<DBDateTime> makeDbArray(long[] result) {
        DBDateTime[] to = new DBDateTime[result.length];

        for (int i = 0; i < result.length; ++i) {
            to[i] = toDBDateTime(result[i]);
        }

        return new DbArrayDirect<>(to);
    }

    private static final class ToDBDateTimePageMillis<ATTR extends Attributes.Any> extends ToDBDateTimePage<ATTR> {
        @Override
        protected DBDateTime toDBDateTime(final long v) {
            return DBTimeUtils.millisToTime(v);
        }
    }

    private static final class ToDBDateTimePageMicros<ATTR extends Attributes.Any> extends ToDBDateTimePage<ATTR> {
        @Override
        protected DBDateTime toDBDateTime(final long v) {
            return DBTimeUtils.microsToTime(v);
        }
    }

    private static final class ToDBDateTimePageNanos<ATTR extends Attributes.Any> extends ToDBDateTimePage<ATTR> {
        @Override
        protected DBDateTime toDBDateTime(final long v) {
            return new DBDateTime(v);
        }
    }
}
