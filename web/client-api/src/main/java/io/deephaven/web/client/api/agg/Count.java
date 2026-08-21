//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import com.vertispan.tsdefs.annotations.TsInterface;
import jsinterop.annotations.JsType;

/**
 * Counts the number of rows in each group. Not supported in aggAllBy.
 */
@JsType
@TsInterface
public final class Count extends Aggregation {
    public final String type = "Count";
    /** The output column name to hold the counts. */
    public String col;
}

