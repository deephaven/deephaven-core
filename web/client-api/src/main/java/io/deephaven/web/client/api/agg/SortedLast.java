//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import com.vertispan.tsdefs.annotations.TsLiteral;
import elemental2.core.ReadonlyArray;
import io.deephaven.web.client.api.Sort;
import jsinterop.annotations.JsType;

/**
 * Returns the last value within each aggregation group when the group is sorted by the specified columns. This is
 * equivalent to sorting each group by the given columns and taking the last row's values.
 */
@JsType
public final class SortedLast extends ColumnAggregation {
    @TsLiteral
    public final String type = "SortedLast";

    /** The columns to sort by when determining which row is "last" within each group. */
    public ReadonlyArray<Sort> sortedColumns;
}
