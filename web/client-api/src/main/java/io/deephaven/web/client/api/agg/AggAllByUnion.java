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
 * Union type for aggregations accepted by {@code aggAllBy}. Only column-based aggregation specs are valid here —
 * non-column types like {@link Count}, {@link CountWhere}, {@link Partition}, {@link FirstRowKey}, and
 * {@link LastRowKey} are not supported.
 */
@JsType(name = "?", namespace = JsPackage.GLOBAL, isNative = true)
@TsUnion
public interface AggAllByUnion {
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

    /**
     * Helper to read the type discriminant from any aggregation variant.
     */
    @JsProperty
    String getType();
}
