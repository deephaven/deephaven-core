//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import com.vertispan.tsdefs.annotations.TsLiteral;
import jsinterop.annotations.JsType;

/**
 * Freezes the value of each group's single row: once set, it does not change when that row updates. A group must have
 * at most one row. A group with more fails the aggregation, so group by columns that are unique in the source. Removing
 * a group removes its output row, and if the group returns, the aggregation freezes its new value.
 */
@JsType(namespace = "dh.agg")
public final class Freeze extends ColumnAggregation {
    @TsLiteral
    public final String type = "Freeze";
}
