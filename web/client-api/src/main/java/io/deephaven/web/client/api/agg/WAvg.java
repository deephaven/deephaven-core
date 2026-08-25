//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import com.vertispan.tsdefs.annotations.TsLiteral;
import io.deephaven.web.client.api.Column.ColumnOrName;
import jsinterop.annotations.JsType;

/**
 * Computes the weighted average within each aggregation group. Each input value is multiplied by the corresponding
 * weight, and the result is the sum of weighted values divided by the sum of weights.
 */
@JsType
public final class WAvg extends ColumnAggregation {
    @TsLiteral
    public final String type = "WAvg";

    /** The column to use as the source of weights for the weighted average calculation. */
    public ColumnOrName weightColumn;
}
