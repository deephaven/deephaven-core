//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import com.vertispan.tsdefs.annotations.TsInterface;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsType;

/**
 * Computes an approximate percentile within each aggregation group using a T-Digest data structure. This is useful for
 * very large data sets where an exact percentile would be too expensive to compute.
 */
@JsType
@TsInterface
public final class ApproxPercentile extends ColumnAggregation {
    public final String type = "ApproxPercentile";

    /** The percentile to calculate. Must be in the range [0.0, 1.0]. */
    public double percentile;

    /**
     * T-Digest compression factor. Must be greater than or equal to 1; values above 1000 are extremely large. Higher
     * values provide more accuracy at the cost of memory and computation. When not specified, the server will choose a
     * default compression value.
     */
    @JsNullable
    public Double compression;
}

