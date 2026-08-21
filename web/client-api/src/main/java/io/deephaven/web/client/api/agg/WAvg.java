//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import com.vertispan.tsdefs.annotations.TsInterface;
import io.deephaven.web.client.api.Column.ColumnOrName;
import jsinterop.annotations.JsType;

@JsType
@TsInterface
public final class WAvg extends ColumnAggregation {
    public final String type = "WAvg";
    public ColumnOrName weightColumn;
}

