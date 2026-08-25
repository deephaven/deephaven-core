//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import com.vertispan.tsdefs.annotations.TsLiteral;
import elemental2.core.ReadonlyArray;
import jsinterop.annotations.JsType;

/**
 * Counts the number of rows matching the given filter expressions within each aggregation group. The count is stored in
 * a new column with the specified name.
 *
 * <p>
 * This aggregation is not supported in {@code aggAllBy} — use it only with {@code aggBy}.
 */
@JsType
public final class CountWhere extends Aggregation {
    @TsLiteral
    public final String type = "CountWhere";

    /** The output column name to hold the filtered row count for each group. */
    public String col;

    /** The filter expression(s) to apply before counting. Only rows matching all filters are counted. */
    public ReadonlyArray<String> filters;
}

