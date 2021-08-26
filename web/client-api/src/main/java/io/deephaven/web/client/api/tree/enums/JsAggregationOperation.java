package io.deephaven.web.client.api.tree.enums;

import jsinterop.annotations.JsType;

@JsType(name = "AggregationOperation", namespace = "dh")
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
            UNIQUE = "Unique",
            // Array operation isn't legal in all contexts, just omit it for now
            // ARRAY = "Array",
            SKIP = "Skip";
}
