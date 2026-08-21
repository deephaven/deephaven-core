//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import com.vertispan.tsdefs.annotations.TsInterface;
import elemental2.core.ReadonlyArray;
import io.deephaven.web.client.api.Sort;
import jsinterop.annotations.JsType;

@JsType
@TsInterface
public final class SortedFirst extends ColumnAggregation {
    public final String type = "SortedFirst";
    public ReadonlyArray<Sort> sortedColumns;
}

