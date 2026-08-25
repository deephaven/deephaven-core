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
import jsinterop.base.Js;

/**
 * Union type representing all aggregation variants accepted by {@code aggBy}. This includes both column-based
 * aggregations (subtypes of {@link ColumnAggregation}) that operate on input/output column pairs, and non-column
 * aggregations ({@link Count}, {@link CountWhere}, {@link Partition}, {@link FirstRowKey}, {@link LastRowKey}) that
 * produce a single named output column.
 *
 * <p>
 * Each variant carries a {@code type} field (accessible via {@link #getType()}) that acts as a discriminant for
 * TypeScript narrowing.
 *
 * @see AggAllByUnion for the subset accepted by {@code aggAllBy}
 */
@JsType(name = "?", namespace = JsPackage.GLOBAL, isNative = true)
@TsUnion
public interface AggregationUnion {
    @JsOverlay
    @TsUnionMember
    default AbsSum asAbsSum() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default ApproxPercentile asApproxPercentile() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default Avg asAvg() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default CountDistinct asCountDistinct() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default Distinct asDistinct() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default First asFirst() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default Formula asFormula() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default Freeze asFreeze() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default Group asGroup() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default Last asLast() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default Max asMax() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default Median asMedian() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default Min asMin() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default Percentile asPercentile() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default SortedFirst asSortedFirst() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default SortedLast asSortedLast() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default Std asStd() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default Sum asSum() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default TDigest asTDigest() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default Unique asUnique() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default Var asVar() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default WAvg asWAvg() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default WSum asWSum() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default Count asCount() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default CountWhere asCountWhere() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default Partition asPartition() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default FirstRowKey asFirstRowKey() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default LastRowKey asLastRowKey() {
        return Js.uncheckedCast(this);
    }

    /**
     * Helper to read the type discriminant from any aggregation variant. Not visible from JS/TS as part of this union,
     * but allows access to the {@code type} field that each aggregation type has.
     */
    @JsProperty
    String getType();
}
