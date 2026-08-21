//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import com.vertispan.tsdefs.annotations.TsInterface;
import jsinterop.annotations.JsType;

/**
 * Computes an exact percentile within each aggregation group.
 */
@JsType
@TsInterface
public final class Percentile extends ColumnAggregation {
    public final String type = "Percentile";

    /** The percentile to calculate. Must be in the range [0.0, 1.0]. */
    public double percentile;

    /**
     * When the percentile splits the group into two equal halves, whether to average the two middle values. When
     * {@code true}, the two middle values are averaged. When {@code false}, the smaller value is used. Only applies to
     * numeric types. Defaults to {@code false}.
     */
    public boolean averageEvenlyDivided;
}
