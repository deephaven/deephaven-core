package io.deephaven.api.agg;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * An aggregation pair represents a {@link #input() input} and {@link #output() output} column for
 * some {@link Aggregation aggregations}. Aggregations that don't have a one-to-one input/output
 * mapping will not need an agg pair.
 */
@Immutable
@SimpleStyle
public abstract class Pair {

    public static Pair of(ColumnName inAndOut) {
        return of(inAndOut, inAndOut);
    }

    public static Pair of(ColumnName input, ColumnName output) {
        return ImmutablePair.of(input, output);
    }

    public static Pair parse(String x) {
        if (ColumnName.isValidParsedColumnName(x)) {
            ColumnName inAndOut = ColumnName.parse(x);
            return of(inAndOut, inAndOut);
        }
        final int ix = x.indexOf('=');
        if (ix < 0) {
            throw new IllegalArgumentException(String.format(
                "Unable to parse agg pair '%s', expected form '<inAndOut>' or '<output>=<input>'",
                x));
        }
        ColumnName output = ColumnName.parse(x.substring(0, ix));
        ColumnName input = ColumnName.parse(x.substring(ix + 1));
        return of(input, output);
    }

    /**
     * The input column.
     *
     * @return the input column
     */
    @Parameter
    public abstract ColumnName input();

    /**
     * The output column.
     *
     * @return the output column
     */
    @Parameter
    public abstract ColumnName output();
}
