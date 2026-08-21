//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import com.vertispan.tsdefs.annotations.TsInterface;
import jsinterop.annotations.JsType;

/**
 * Returns the row key of the first row in each group. Not supported in aggAllBy.
 */
@JsType
@TsInterface
public final class FirstRowKey extends Aggregation {
    public final String type = "FirstRowKey";
    /** The output column name to hold the first row key. */
    public String col;
}

