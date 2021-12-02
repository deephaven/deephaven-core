package io.deephaven.engine.table.impl.by;

import io.deephaven.engine.util.TotalsTableBuilder;

/**
 * Enumeration representing valid aggregation types for {@link AggregationFactory} or {@link TotalsTableBuilder}.
 */
public enum AggType {
    /** Return the number of rows in each group. */
    Count,
    /** Return the minimum value of each group. */
    Min,
    /** Return the maximum value of each group. */
    Max,
    /** Return the sum of values in each group. */
    Sum,
    /** Return the sum of absolute values in each group. */
    AbsSum,
    /** Return the variance of values in each group. */
    Var,
    /** Return the average of values in each group. */
    Avg,
    /** Return the standard deviation of each group. */
    Std,
    /** Return the first value of each group. */
    First,
    /** Return the last value of each group. */
    Last,
    /** Return the values of each group as a Vector. */
    Group,
    /** Return the number of unique values in each group */
    CountDistinct,
    /** Collect the distinct items from the column */
    Distinct,
    /**
     * Display the singular value from the column if it is unique, or a default value if none are present, or it is not
     * unique
     */
    Unique,
    /** Only valid in a TotalsTableBuilder to indicate we should not perform any aggregation. */
    Skip
}
