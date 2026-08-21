//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import com.vertispan.tsdefs.annotations.TsInterface;
import jsinterop.annotations.JsType;

/**
 * Computes the median value within each aggregation group.
 */
@JsType
@TsInterface
public final class Median extends ColumnAggregation {
    public final String type = "Median";

    /**
     * When the group size is an even number, whether to average the two middle values for the output value. When
     * {@code true}, the two middle values are averaged. When {@code false}, the smaller value is used. Only applies to
     * numeric types. Defaults to {@code true}.
     */
    public boolean averageEvenlyDivided;
}
