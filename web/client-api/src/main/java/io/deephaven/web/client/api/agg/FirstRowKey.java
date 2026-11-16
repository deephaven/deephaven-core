//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import com.vertispan.tsdefs.annotations.TsLiteral;
import jsinterop.annotations.JsType;

/**
 * Returns the row key of the first row in each aggregation group, in a new column with the specified name.
 *
 * <p>
 * This aggregation is not supported in {@link io.deephaven.web.client.api.JsTableOperations#aggAllBy aggAllBy} — use it
 * only with {@link io.deephaven.web.client.api.JsTableOperations#aggBy aggBy}.
 */
@JsType(namespace = "dh.agg")
public final class FirstRowKey extends Aggregation {
    @TsLiteral
    public final String type = "FirstRowKey";

    /** The output column name to hold the first row key for each group. */
    public String col;
}

