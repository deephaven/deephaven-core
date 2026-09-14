//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import com.vertispan.tsdefs.annotations.TsLiteral;
import jsinterop.annotations.JsType;

/**
 * Counts the number of rows in each aggregation group. The count is stored in a new column with the specified name.
 *
 * <p>
 * This aggregation is not supported in {@code aggAllBy} — use it only with {@code aggBy}.
 */
@JsType(namespace = "dh.agg")
public final class Count extends Aggregation {
    @TsLiteral
    public final String type = "Count";

    /** The output column name to hold the row count for each group. */
    public String col;
}

