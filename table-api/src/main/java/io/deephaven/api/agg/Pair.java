package io.deephaven.api.agg;

import io.deephaven.api.ColumnName;

import java.io.Serializable;

/**
 * A column pair represents an {@link #input() input} and an {@link #output() output} column.
 *
 * @see ColumnAggregation
 * @see ColumnAggregations
 */
public interface Pair extends Serializable {

    static Pair of(ColumnName input, ColumnName output) {
        if (input.equals(output)) {
            return input;
        }
        return PairImpl.of(input, output);
    }

    static Pair parse(String x) {
        final int ix = x.indexOf('=');
        if (ix < 0) {
            return ColumnName.parse(x);
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
