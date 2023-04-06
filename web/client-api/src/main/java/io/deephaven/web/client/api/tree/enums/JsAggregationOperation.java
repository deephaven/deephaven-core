/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api.tree.enums;

import com.vertispan.tsdefs.annotations.TsTypeDef;
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
}
