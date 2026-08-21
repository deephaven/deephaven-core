//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import com.vertispan.tsdefs.annotations.TsInterface;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsType;

/**
 * Approximate percentile aggregation using T-Digest.
 */
@JsType
@TsInterface
public final class ApproxPercentile extends ColumnAggregation {
    public final String type = "ApproxPercentile";
    /**
     * The percentile to calculate. Must be in the range [0.0, 1.0].
     */
    public double percentile;
    /**
     * T-Digest compression factor. Must be greater than or equal to 1. 1000 is extremely large. When not specified, the
     * server will choose a compression value.
     */
    @JsNullable
    public Double compression;
}

