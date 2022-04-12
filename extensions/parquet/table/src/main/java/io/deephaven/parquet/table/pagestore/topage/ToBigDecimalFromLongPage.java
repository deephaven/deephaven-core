package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.attributes.Any;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;

import static io.deephaven.util.QueryConstants.NULL_LONG_BOXED;

public class ToBigDecimalFromLongPage<ATTR extends Any> extends ToBigDecimalBase<ATTR> {

    public static <ATTR extends Any> ToPage<ATTR, BigDecimal[]> create(
            @NotNull final Class<?> nativeType,
            final int precision,
            final int scale
    ) {
        return new ToBigDecimalFromLongPage(nativeType, precision, scale);
    }

    protected ToBigDecimalFromLongPage(@NotNull final Class<?> nativeType, final int precision, final int scale) {
        super(nativeType, precision, scale);
    }

    @Override
    public BigDecimal[] convertResult(@NotNull final Object result) {
        final long[] in = (long[]) result;
        final int resultLength = in.length;
        final BigDecimal[] out = new BigDecimal[resultLength];
        for (int ri = 0; ri < resultLength; ++ri) {
            out[ri] = BigDecimal.valueOf(in[ri], scale);
        }
        return out;
    }

    @Override
    @NotNull
    public final Object nullValue() {
        return NULL_LONG_BOXED;
    }
}
