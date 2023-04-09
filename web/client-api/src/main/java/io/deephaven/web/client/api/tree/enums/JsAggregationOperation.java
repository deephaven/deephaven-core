/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api.tree.enums;

import com.vertispan.tsdefs.annotations.TsTypeDef;
import jsinterop.annotations.JsType;

/**
 * This enum describes the name of each supported operation/aggregation type when creating a `TreeTable`.
 */
@JsType(name = "AggregationOperation", namespace = "dh")
@TsTypeDef(tsType = "string")
public class JsAggregationOperation {
    /**
     * The total number of values in the specified column. Can apply to any type. String value is "Count".
     */
    public static final String COUNT = "Count",
            /**
             * Return the number of unique values in each group. Can apply to any type. String value is "CountDistinct".
             */
            COUNT_DISTINCT = "CountDistinct",
            /**
             * Collect the distinct items from the column. Can apply to any type. String value is "Distinct".
             */
            DISTINCT = "Distinct",
            /**
             * The minimum value in the specified column. Can apply to any column type which is Comparable in Java.
             * String value is "Min".
             */
            MIN = "Min",
            /**
             * The maximum value in the specified column. Can apply to any column type which is Comparable in Java.
             * String value is "Max".
             */
            MAX = "Max",
            /**
             * The sum of all values in the specified column. Can only apply to numeric types. String value is "Sum".
             */
            SUM = "Sum",
            /**
             * The sum of all values, as their distance from zero, in the specified column. Can only apply to numeric
             * types. String value is “AbsSum”.
             */
            ABS_SUM = "AbsSum",
            /**
             * The variance of all values in the specified column. Can only apply to numeric types. String value is
             * "Var".
             */
            VAR = "Var",
            /**
             * The average of all values in the specified column. Can only apply to numeric types. String value is
             * "Avg".
             */
            AVG = "Avg",
            /**
             * The standard deviation of all values in the specified column. Can only apply to numeric types. String
             * value is "Std".
             */
            STD = "Std",
            /**
             * The first value in the specified column. Can apply to any type. String value is "First".
             */
            FIRST = "First",
            /**
             * The last value in the specified column. Can apply to any type. String value is "Last".
             */
            LAST = "Last",
            UNIQUE = "Unique";
    /**
     * Indicates that this column should not be aggregated. String value is "Skip".
     */
    // Array operation isn't legal in all contexts, just omit it for now
    // ARRAY = "Array",
    // These need some other parameter to function, not supported yet
    // TODO #3302 support these
    // SORTED_FIRST="SortedFirst",
    // SORTED_LAST="SortedLast",
    // WSUM = "WeightedSum";
    @Deprecated
    public static final String SKIP = "Skip";
}
