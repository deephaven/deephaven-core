//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import com.vertispan.tsdefs.annotations.TsLiteral;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsType;

/**
 * Computes a T-Digest data structure within each aggregation group, which can be used later for approximate percentile
 * calculations. This stores a compressed representation of the data distribution.
 */
@JsType
public final class TDigest extends ColumnAggregation {
    @TsLiteral
    public final String type = "TDigest";

    /**
     * T-Digest compression factor. Must be greater than or equal to 1; values above 1000 are extremely large. Higher
     * values provide more accuracy at the cost of memory and computation. When not specified, the server will choose a
     * default compression value.
     */
    @JsNullable
    public Double compression;
}
