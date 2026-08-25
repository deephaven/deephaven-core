//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import com.vertispan.tsdefs.annotations.TsLiteral;
import jsinterop.annotations.JsType;

/**
 * Applies a user-defined formula to the values within each aggregation group. The formula is evaluated once per group,
 * with the column's grouped values available as a vector via the parameter token.
 *
 * <p>
 * For example, with {@code formula = "max(each)"} and {@code paramToken = "each"}, the formula will compute the maximum
 * of each group's values, where {@code each} is replaced by the column's vector for that group.
 */
@JsType(namespace = "dh.agg")
public final class Formula extends ColumnAggregation {
    @TsLiteral
    public final String type = "Formula";

    /** The formula expression to evaluate for each group. */
    public String formula;

    /**
     * The parameter token in the formula that will be replaced with the input column name for evaluation. For example,
     * if the formula is {@code "max(each)"}, then {@code paramToken} should be {@code "each"}.
     */
    public String paramToken;
}
