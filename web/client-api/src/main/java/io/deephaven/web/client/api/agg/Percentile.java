//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import com.vertispan.tsdefs.annotations.TsInterface;
import jsinterop.annotations.JsType;

@JsType
@TsInterface
public final class Percentile extends ColumnAggregation {
    public final String type = "Percentile";
    public double percentile;
    public boolean averageEvenlyDivided;
}

