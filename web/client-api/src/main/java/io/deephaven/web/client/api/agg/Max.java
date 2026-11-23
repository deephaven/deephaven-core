//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import com.vertispan.tsdefs.annotations.TsLiteral;
import jsinterop.annotations.JsType;

/**
 * Computes the maximum value within each aggregation group.
 */
@JsType(namespace = "dh.agg")
public final class Max extends ColumnAggregation {
    @TsLiteral
    public final String type = "Max";
}
