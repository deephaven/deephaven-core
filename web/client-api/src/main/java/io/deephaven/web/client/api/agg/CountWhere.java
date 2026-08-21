//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import com.vertispan.tsdefs.annotations.TsInterface;
import elemental2.core.ReadonlyArray;
import jsinterop.annotations.JsType;

/**
 * Counts the number of rows matching the given filters in each group. Not supported in aggAllBy.
 */
@JsType
@TsInterface
public final class CountWhere extends Aggregation {
    public final String type = "CountWhere";
    /** The output column name to hold the counts. */
    public String col;
    /** The filter expression(s) to apply. */
    public ReadonlyArray<String> filters;
}

