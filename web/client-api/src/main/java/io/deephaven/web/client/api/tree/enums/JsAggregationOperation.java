/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api.tree.enums;

import io.deephaven.web.client.fu.JsLog;
import jsinterop.annotations.JsProperty;
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
            UNIQUE = "Unique";
    // Array operation isn't legal in all contexts, just omit it for now
    // ARRAY = "Array",
    // These need some other parameter to function, not supported yet
    // SORTED_FIRST="SortedFirst",
    // SORTED_LAST="SortedLast",
    // WSUM = "WeightedSum";

    @JsProperty(name = "SKIP")
    public static String getSKIP() {
        JsLog.warn("SKIP is not supported in deephaven-core");
        return "Skip";
    }
}
