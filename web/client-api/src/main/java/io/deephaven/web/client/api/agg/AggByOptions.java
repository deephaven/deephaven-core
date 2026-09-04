//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import com.vertispan.tsdefs.annotations.TsInterface;
import elemental2.core.ReadonlyArray;
import io.deephaven.web.client.api.Column;
import io.deephaven.web.client.api.JsTableOperations;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsType;
import org.jetbrains.annotations.Nullable;

/**
 * Options to control a call to {@link JsTableOperations#aggBy(AggByOptions)}.
 */
@TsInterface
@JsType(namespace = "dh.agg")
public class AggByOptions {
    /**
     * The aggregations to apply
     */
    public ReadonlyArray<AggregationUnion> aggregations;

    /**
     * Whether to keep result rows for groups that are initially empty or become empty as a result of updates. Each
     * aggregation operator defines its own values for empty groups. Defaults to false if unspecified.
     */
    @Nullable
    public Boolean preserveEmpty;

    /**
     * An optional table whose distinct combinations of values for the {@link #groupByColumns} should be used to create
     * an initial set of aggregation groups. All other columns are ignored.
     * <p>
     * Only the initial state of this table is used, updates will be ignored.
     */
    @JsNullable
    public JsTableOperations initialGroups;

    /**
     * The columns to group by. Must be specified if {@link #initialGroups} is non-null. If empty or unspecified, the
     * result will be a single group containing all rows in the table.
     */
    @JsNullable
    public ReadonlyArray<Column.ColumnOrName> groupByColumns;
}
