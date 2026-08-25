//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import com.vertispan.tsdefs.annotations.TsLiteral;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsType;

/**
 * Computes the distinct values within each aggregation group and stores them as arrays/vectors.
 */
@JsType
public final class Distinct extends ColumnAggregation {
    @TsLiteral
    public final String type = "Distinct";

    /**
     * Whether {@code null} values should be included in the distinct output values. Defaults to {@code false}.
     */
    @JsNullable
    public Boolean includeNulls;
}
