//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import com.vertispan.tsdefs.annotations.TsUnion;
import com.vertispan.tsdefs.annotations.TsUnionMember;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;

/**
 * Union type for all aggregations accepted by most APIs
 */
@JsType(name = "?", namespace = JsPackage.GLOBAL, isNative = true)
@TsUnion
public interface AggregationUnion {
    @JsOverlay
    @TsUnionMember
    AbsSum asAbsSum();

    @JsOverlay
    @TsUnionMember
    ApproxPercentile asApproxPercentile();

    @JsOverlay
    @TsUnionMember
    Avg asAvg();

    @JsOverlay
    @TsUnionMember
    CountDistinct asCountDistinct();

    @JsOverlay
    @TsUnionMember
    Distinct asDistinct();

    @JsOverlay
    @TsUnionMember
    First asFirst();

    @JsOverlay
    @TsUnionMember
    Formula asFormula();

    @JsOverlay
    @TsUnionMember
    Freeze asFreeze();

    @JsOverlay
    @TsUnionMember
    Group asGroup();

    @JsOverlay
    @TsUnionMember
    Last asLast();

    @JsOverlay
    @TsUnionMember
    Max asMax();

    @JsOverlay
    @TsUnionMember
    Median asMedian();

    @JsOverlay
    @TsUnionMember
    Min asMin();

    @JsOverlay
    @TsUnionMember
    Percentile asPercentile();

    @JsOverlay
    @TsUnionMember
    SortedFirst asSortedFirst();

    @JsOverlay
    @TsUnionMember
    SortedLast asSortedLast();

    @JsOverlay
    @TsUnionMember
    Std asStd();

    @JsOverlay
    @TsUnionMember
    Sum asSum();

    @JsOverlay
    @TsUnionMember
    TDigest asTDigest();

    @JsOverlay
    @TsUnionMember
    Unique asUnique();

    @JsOverlay
    @TsUnionMember
    Var asVar();

    @JsOverlay
    @TsUnionMember
    WAvg asWAvg();

    @JsOverlay
    @TsUnionMember
    WSum asWSum();

    @JsOverlay
    @TsUnionMember
    Count asCount();

    @JsOverlay
    @TsUnionMember
    CountWhere asCountWhere();

    @JsOverlay
    @TsUnionMember
    Partition asPartition();

    @JsOverlay
    @TsUnionMember
    FirstRowKey asFirstRowKey();

    @JsOverlay
    @TsUnionMember
    LastRowKey asLastRowKey();

    /**
     * Helper to read the type discriminant from any aggregation variant. Not visible from JS/TS as part of this union,
     * but allows access to the {@code type} field that each aggregation type has.
     */
    @JsProperty
    String getType();
}

