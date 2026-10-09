//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import com.vertispan.tsdefs.annotations.TsLiteral;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsType;

/**
 * Counts the number of distinct values within each aggregation group.
 */
@JsType(namespace = "dh.agg")
public final class CountDistinct extends ColumnAggregation {
    @TsLiteral
    public final String type = "CountDistinct";

    /**
     * Whether {@code null} values should be included when counting distinct values. Defaults to {@code false}.
     */
    @JsNullable
    public Boolean countNulls;
}
