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
 * Union type for aggregations accepted by {@code aggAllBy}. Only column-based aggregation specs are valid here —
 * non-column types like {@link Count}, {@link CountWhere}, {@link Partition}, {@link FirstRowKey}, and
 * {@link LastRowKey} are not supported.
 */
@JsType(name = "?", namespace = JsPackage.GLOBAL, isNative = true)
@TsUnion
public interface AggAllByUnion {
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

    /**
     * Helper to read the type discriminant from any aggregation variant.
     */
    @JsProperty
    String getType();
}

