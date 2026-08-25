//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import com.vertispan.tsdefs.annotations.TsLiteral;
import io.deephaven.web.client.api.Column.ColumnOrName;
import jsinterop.annotations.JsType;

/**
 * Computes the weighted sum within each aggregation group. Each input value is multiplied by the corresponding weight,
 * and the results are summed.
 */
@JsType
public final class WSum extends ColumnAggregation {
    @TsLiteral
    public final String type = "WSum";

    /** The column to use as the source of weights for the weighted sum calculation. */
    public ColumnOrName weightColumn;
}
