//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import elemental2.core.ReadonlyArray;
import io.deephaven.web.client.api.JsTable.MatchPairUnion;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsType;

/**
 * Base type for aggregations that operate on columns via an {@code AggSpec} and optional input/output column match-pair
 * mappings. When used with {@code aggAllBy}, the {@link #columns} field is ignored and the spec is applied to every
 * non-key column. When used with {@code aggBy}, the {@link #columns} field specifies which columns to aggregate, and
 * may include renaming expressions (e.g. {@code "OutputCol = InputCol"}).
 *
 * <p>
 * If {@code columns} is null or empty in an {@code aggBy} call, the aggregation applies to all non-key columns.
 */
@JsType
public sealed class ColumnAggregation extends Aggregation
        permits AbsSum, ApproxPercentile, Avg, CountDistinct, Distinct, First, Formula, Freeze, Group, Last, Max,
        Median, Min, Percentile, SortedFirst, SortedLast, Std, Sum, TDigest, Unique, Var, WAvg, WSum {

    /**
     * The column(s) to aggregate, which can be renaming expressions (e.g. {@code "new_col = col"}). When null or empty,
     * the aggregation applies to all non-key columns (valid only in {@code aggAllBy}).
     */
    @JsNullable
    public ReadonlyArray<MatchPairUnion> columns;
}
