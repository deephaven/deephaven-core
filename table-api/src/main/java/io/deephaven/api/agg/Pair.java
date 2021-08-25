package io.deephaven.api.agg;

import io.deephaven.api.ColumnName;

import java.io.Serializable;

/**
 * An aggregation pair represents a {@link #input() input} and {@link #output() output} column for some
 * {@link Aggregation aggregations}. Aggregations that don't have a one-to-one input/output mapping will not need an agg
 * pair.
 */
public interface Pair extends Serializable {

    static Pair of(ColumnName input, ColumnName output) {
        if (input.equals(output)) {
            return input;
        }
        return PairImpl.of(input, output);
    }

    static Pair parse(String x) {
        if (ColumnName.isValidParsedColumnName(x)) {
            return ColumnName.parse(x);
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
    ColumnName input();

    /**
     * The output column.
     *
     * @return the output column
     */
    ColumnName output();
}
