//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import com.vertispan.tsdefs.annotations.TsLiteral;
import jsinterop.annotations.JsType;

/**
 * An aggregation that computes one output column per group from a formula written as {@code "Out = expression"}. Within
 * the expression, each input column stands for that column's values in the group, as a vector, so the expression
 * applies functions that reduce them, as in {@code "MaxW = max(W)"} or {@code "Spread = max(W) - min(W)"}.
 */
@JsType(namespace = "dh.agg")
public final class Formula extends Aggregation {
    @TsLiteral
    public final String type = "Formula";

    /**
     * The output column's name and the expression that computes it, as {@code "Out = expression"}.
     */
    public String formula;
}
