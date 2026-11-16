//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import com.vertispan.tsdefs.annotations.TsLiteral;
import jsinterop.annotations.JsType;

/**
 * Counts the number of rows in each aggregation group, and writes the count to a new column with the specified name.
 *
 * <p>
 * This aggregation is not supported in {@link io.deephaven.web.client.api.JsTableOperations#aggAllBy aggAllBy} — use it
 * only with {@link io.deephaven.web.client.api.JsTableOperations#aggBy aggBy}.
 */
@JsType(namespace = "dh.agg")
public final class Count extends Aggregation {
    @TsLiteral
    public final String type = "Count";

    /** The output column name to hold the row count for each group. */
    public String col;
}

