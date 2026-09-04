//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import com.vertispan.tsdefs.annotations.TsLiteral;
import jsinterop.annotations.JsType;

/**
 * Collects all values within each aggregation group into an array/vector column.
 */
@JsType(namespace = "dh.agg")
public final class Group extends ColumnAggregation {
    @TsLiteral
    public final String type = "Group";
}
