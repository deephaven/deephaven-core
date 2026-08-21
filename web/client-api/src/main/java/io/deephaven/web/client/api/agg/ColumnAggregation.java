//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import com.vertispan.tsdefs.annotations.TsInterface;
import elemental2.core.ReadonlyArray;
import io.deephaven.web.client.api.Column.ColumnOrName;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsType;

/**
 * Base type for aggregations that operate on columns via an {@link io.deephaven.proto.backplane.grpc.AggSpec AggSpec}
 * and optional match-pair column mappings.
 */
@JsType
@TsInterface
public sealed class ColumnAggregation extends Aggregation
        permits AbsSum, ApproxPercentile, Avg, CountDistinct, Distinct, First, Formula, Freeze, Group, Last, Max,
        Median, Min, Percentile, SortedFirst, SortedLast, Std, Sum, TDigest, Unique, Var, WAvg, WSum {

    // TODO support match pair, not just name
    @JsNullable
    public ReadonlyArray<ColumnOrName> columns;
}

