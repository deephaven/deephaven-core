/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api.tree.enums;

import com.vertispan.tsdefs.annotations.TsTypeDef;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;

@JsType(name = "AggregationOperation", namespace = "dh")
@TsTypeDef(tsType = "string")
public class JsAggregationOperation {
    public static final String COUNT = "Count",
            COUNT_DISTINCT = "CountDistinct",
            DISTINCT = "Distinct",
            MIN = "Min",
            MAX = "Max",
            SUM = "Sum",
            ABS_SUM = "AbsSum",
            VAR = "Var",
            AVG = "Avg",
            STD = "Std",
            FIRST = "First",
            LAST = "Last",
            UNIQUE = "Unique";
    // Array operation isn't legal in all contexts, just omit it for now
    // ARRAY = "Array",
    // These need some other parameter to function, not supported yet
    // TODO #3302 support these
    // SORTED_FIRST="SortedFirst",
    // SORTED_LAST="SortedLast",
    // WSUM = "WeightedSum";
    @Deprecated
    public static final String SKIP = "Skip";

    @JsIgnore
    public static boolean canAggregateType(String aggregationType, String columnType) {
        switch (aggregationType) {
            case COUNT:
            case COUNT_DISTINCT:
            case DISTINCT:
            case FIRST:
            case LAST:
            case UNIQUE: {
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
