//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import com.vertispan.tsdefs.annotations.TsLiteral;
import jsinterop.annotations.JsType;

/**
 * Returns the last value within each aggregation group.
 */
@JsType
public final class Last extends ColumnAggregation {
    @TsLiteral
    public final String type = "Last";
}
