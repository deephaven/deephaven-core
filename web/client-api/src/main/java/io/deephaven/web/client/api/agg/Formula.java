//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import com.vertispan.tsdefs.annotations.TsLiteral;
import jsinterop.annotations.JsType;

/**
 * An aggregation that applies a user-defined formula.
 */
@JsType(namespace = "dh.agg")
public final class Formula extends Aggregation {
    @TsLiteral
    public final String type = "Formula";

    /**
     * The formula to use to calculate output values.
     */
    public String formula;
}
