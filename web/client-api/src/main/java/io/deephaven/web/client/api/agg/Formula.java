//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import com.vertispan.tsdefs.annotations.TsInterface;
import jsinterop.annotations.JsType;

@JsType
@TsInterface
public final class Formula extends ColumnAggregation {
    public final String type = "Formula";
    public String formula;
    public String paramToken;
}

