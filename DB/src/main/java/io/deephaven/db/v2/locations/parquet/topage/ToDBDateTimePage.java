package io.deephaven.db.v2.locations.parquet.topage;

import io.deephaven.db.tables.dbarrays.DbArray;
import io.deephaven.db.tables.dbarrays.DbArrayDirect;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.v2.sources.chunk.Attributes;
import org.jetbrains.annotations.NotNull;

public class ToDBDateTimePage<ATTR extends Attributes.Any> extends ToLongPage<ATTR> {

    private static ToDBDateTimePage INSTANCE = new ToDBDateTimePage<>();

    public static <ATTR extends Attributes.Any> ToDBDateTimePage<ATTR> create(@NotNull Class<?> nativeType) {
        if (DBDateTime.class.equals(nativeType)) {
            //noinspection unchecked
            return INSTANCE;
        }

        throw new IllegalArgumentException("The native type for a Int column is " + nativeType.getCanonicalName());
    }

    private ToDBDateTimePage() {}

    @Override
    @NotNull
    public final Class<DBDateTime> getNativeComponentType() {
        return DBDateTime.class;
    }

    @Override
    @NotNull
    public DbArray<DBDateTime> makeDbArray(long [] result) {
        DBDateTime[] to = new DBDateTime[result.length];

        for (int i = 0; i < result.length; ++i) {
            to[i] = new DBDateTime(result[i]);
        }

        return new DbArrayDirect<>(to);
    }
}
