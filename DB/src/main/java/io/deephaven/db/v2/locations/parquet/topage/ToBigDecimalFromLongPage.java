package io.deephaven.db.v2.locations.parquet.topage;

import io.deephaven.db.tables.dbarrays.DbArray;
import io.deephaven.db.tables.dbarrays.DbArrayDirect;
import io.deephaven.db.v2.sources.chunk.Attributes;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;

public class ToBigDecimalFromLongPage<ATTR extends Attributes.Any> extends ToLongPage<ATTR> {
    private final byte scale;

    public static <ATTR extends Attributes.Any> ToPage<ATTR, BigDecimal[]> create(
            @NotNull final Class<?> nativeType,
            final int precision,
            final int scale
    ) {
        if (!BigDecimal.class.equals(nativeType)) {
            throw new IllegalArgumentException(
                    "The native type for a BigDecimal column is " + nativeType.getCanonicalName());
        }

        return new ToBigDecimalFromLongPage(precision, scale);
    }

    protected ToBigDecimalFromLongPage(final int precision, final int scale) {
        this.scale = (byte) scale;
        if (((int) this.scale) != scale) {
            throw new IllegalArgumentException("precision=" + precision + " and scale=" + scale + " can't be represented");
        }
    }

    @Override
    @NotNull
    public DbArray<BigDecimal> makeDbArray(final long[] result) {
        final BigDecimal[] to = new BigDecimal[result.length];
        for (int i = 0; i < result.length; ++i) {
            to[i] = BigDecimal.valueOf(result[i], scale);
        }
        return new DbArrayDirect<>(to);
    }

    @Override
    public long[] convertResult(@NotNull final Object result) {
        return (long[]) result;
    }
}
