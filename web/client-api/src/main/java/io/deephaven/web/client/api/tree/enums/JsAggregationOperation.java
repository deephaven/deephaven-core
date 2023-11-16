/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api.tree.enums;

import com.vertispan.tsdefs.annotations.TsTypeDef;
import jsinterop.annotations.JsIgnore;
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
             * The sample variance of all values in the specified column. Can only apply to numeric types. String value
             * is "Var".
             *
             * Sample variance is computed using Bessel's correction
             * (https://en.wikipedia.org/wiki/Bessel%27s_correction), which ensures that the sample variance will be an
             * unbiased estimator of population variance.
             */
            VAR = "Var",
            /**
             * The average of all values in the specified column. Can only apply to numeric types. String value is
             * "Avg".
             */
            AVG = "Avg",
            /**
             * The sample standard deviation of all values in the specified column. Can only apply to numeric types.
             * String value is "Std". Sample standard deviation is computed using Bessel's correction
             * (https://en.wikipedia.org/wiki/Bessel%27s_correction), which ensures that the sample variance will be an
             * unbiased estimator of population variance.
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

    // Array operation isn't legal in all contexts, just omit it for now
    // ARRAY = "Array",
    // These need some other parameter to function, not supported yet
    // TODO #3302 support these
    // SORTED_FIRST="SortedFirst",
    // SORTED_LAST="SortedLast",
    // WSUM = "WeightedSum";

    /**
     * Indicates that this column should not be aggregated. String value is "Skip".
     */
    public static final String SKIP = "Skip";

    @JsIgnore
    public static boolean canAggregateType(String aggregationType, String columnType) {
        switch (aggregationType) {
            case COUNT:
            case COUNT_DISTINCT:
            case DISTINCT:
            case FIRST:
            case LAST:
            case UNIQUE:
            case SKIP: {
                // These operations are always safe
                return true;
            }
            case ABS_SUM:
            case SUM: {
                // Both sum operators will count "true" boolean values
                return isNumericOrBoolean(columnType);
            }
            case AVG:
            case VAR:
            case STD: {
                return isNumeric(columnType);
            }
            case MIN:
            case MAX: {
                // Can only apply to Comparables - JS can't work this out, so we'll stick to known types
                return isComparable(columnType);
            }
        }
        return false;
    }

    private static boolean isNumeric(String columnType) {
        switch (columnType) {
            case "double":
            case "float":
            case "int":
            case "long":
            case "short":
            case "char":
            case "byte": {
                return true;
            }
        }
        return false;
    }

    private static boolean isNumericOrBoolean(String columnType) {
        if (isNumeric(columnType)) {
            return true;
        }
        return columnType.equals("boolean") || columnType.equals("java.lang.Boolean");
    }

    private static boolean isComparable(String columnType) {
        if (isNumericOrBoolean(columnType)) {
            return true;
        }
        switch (columnType) {
            case "java.lang.String":
            case "java.time.Instant":
            case "java.time.ZonedDateTime":
            case "io.deephaven.time.DateTime":
            case "java.time.LocalTime":
            case "java.time.LocalDate":
            case "java.math.BigDecimal":
            case "java.math.BigInteger":
                return true;
        }
        return false;
    }

}
