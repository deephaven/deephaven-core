//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import com.vertispan.tsdefs.annotations.TsInterface;
import elemental2.core.ReadonlyArray;
import io.deephaven.web.client.api.Sort;
import jsinterop.annotations.JsType;

/**
 * Returns the first value within each aggregation group when the group is sorted by the specified columns. This is
 * equivalent to sorting each group by the given columns and taking the first row's values.
 */
@JsType
@TsInterface
public final class SortedFirst extends ColumnAggregation {
    public final String type = "SortedFirst";

    /** The columns to sort by when determining which row is "first" within each group. */
    public ReadonlyArray<Sort> sortedColumns;
}
