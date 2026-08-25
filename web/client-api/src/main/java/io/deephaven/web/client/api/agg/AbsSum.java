//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import com.vertispan.tsdefs.annotations.TsLiteral;
import jsinterop.annotations.JsType;

/**
 * Computes the sum of absolute values within each aggregation group.
 */
@JsType
public final class AbsSum extends ColumnAggregation {
    @TsLiteral
    public final String type = "AbsSum";
}
