//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import com.vertispan.tsdefs.annotations.TsInterface;
import jsinterop.annotations.JsType;

/**
 * Returns the row key of the first row in each aggregation group. The row key is stored in a new column with the
 * specified name.
 *
 * <p>
 * This aggregation is not supported in {@code aggAllBy} — use it only with {@code aggBy}.
 */
@JsType
@TsInterface
public final class FirstRowKey extends Aggregation {
    public final String type = "FirstRowKey";

    /** The output column name to hold the first row key for each group. */
    public String col;
}

