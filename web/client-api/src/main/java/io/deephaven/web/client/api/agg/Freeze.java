//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import com.vertispan.tsdefs.annotations.TsInterface;
import jsinterop.annotations.JsType;

/**
 * Freezes the first value seen for each group. Once a value has been set for a group, it will not change even if the
 * underlying data updates. This is useful for capturing an initial state.
 */
@JsType
@TsInterface
public final class Freeze extends ColumnAggregation {
    public final String type = "Freeze";
}
