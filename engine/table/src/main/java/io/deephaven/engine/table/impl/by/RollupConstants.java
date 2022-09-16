/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.by;

/**
 * Implementation constants for {@link io.deephaven.engine.table.Table#rollup} support.
 */
public final class RollupConstants {

    private RollupConstants() {}

    /**
     * Marker suffix for rollup-internal column names.
     */
    public static final String ROLLUP_INTERNAL_COLUMN_SUFFIX = "__ROLLUP__";

    /**
     * Marker suffix for rollup-constituent column names.
     */
    public static final String ROLLUP_CONSTITUENT_COLUMN_SUFFIX = "__CONSTITUENT__";

    /**
     * Prefix for row redirection columns.
     */
    static final String ROW_REDIRECTION_PREFIX = "RowRedirection_";

    /**
     * Middle column name component (between source column name and {@link #ROLLUP_INTERNAL_COLUMN_SUFFIX suffix}) for
     * SSM columns used in "distinct", "count distinct", and "unique" rollup aggregations.
     */
    public static final String ROLLUP_DISTINCT_SSM_COLUMN_ID = "_SSM_";

    /**
     * Middle column name component (between source column name and {@link #ROLLUP_INTERNAL_COLUMN_SUFFIX suffix}) for
     * running sum columns used in rollup aggregations.
     */
    static final String ROLLUP_RUNNING_SUM_COLUMN_ID = "_RS_";

    /**
     * Middle column name component (between source column name and {@link #ROLLUP_INTERNAL_COLUMN_SUFFIX suffix}) for
     * running sum of squares columns used in rollup aggregations.
     */
    static final String ROLLUP_RUNNING_SUM2_COLUMN_ID = "_RS2_";

    /**
     * Middle column name component (between source column name and {@link #ROLLUP_INTERNAL_COLUMN_SUFFIX suffix}) for
     * non-null count columns used in rollup aggregations.
     */
    static final String ROLLUP_NONNULL_COUNT_COLUMN_ID = "_NNC_";

    /**
     * Middle column name component (between source column name and {@link #ROLLUP_INTERNAL_COLUMN_SUFFIX suffix}) for
     * NaN count columns used in rollup aggregations.
     */
    static final String ROLLUP_NAN_COUNT_COLUMN_ID = "_NaNC_";

    /**
     * Middle column name component (between source column name and {@link #ROLLUP_INTERNAL_COLUMN_SUFFIX suffix}) for
     * positive infinity count columns used in rollup aggregations.
     */
    static final String ROLLUP_PI_COUNT_COLUMN_ID = "_PIC_";

    /**
     * Middle column name component (between source column name and {@link #ROLLUP_INTERNAL_COLUMN_SUFFIX suffix}) for
     * negative infinity count columns used in rollup aggregations.
     */
    static final String ROLLUP_NI_COUNT_COLUMN_ID = "_NIC_";
}
