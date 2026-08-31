//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import com.vertispan.tsdefs.annotations.TsUnion;
import com.vertispan.tsdefs.annotations.TsUnionMember;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

@TsUnion
@JsType(name = "?", namespace = JsPackage.GLOBAL, isNative = true)
public interface UpdateBySpecUnion {
    @JsOverlay
    @TsUnionMember
    default UpdateByCumulativeSum asCumulativeSum() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByCumulativeMin asCumulativeMin() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByCumulativeMax asCumulativeMax() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByCumulativeProduct asCumulativeProduct() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByFill asFill() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByEma asEma() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByRollingSum asRollingSum() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByRollingGroup asRollingGroup() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByRollingAvg asRollingAvg() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByRollingMin asRollingMin() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByRollingMax asRollingMax() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByRollingProduct asRollingProduct() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByDelta asDelta() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByEms asEms() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByEmMin asEmMin() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByEmMax asEmMax() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByEmStd asEmStd() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByRollingCount asRollingCount() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByRollingStd asRollingStd() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByRollingWAvg asRollingWAvg() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByRollingFormula asRollingFormula() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByRollingCountWhere asRollingCountWhere() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default UpdateByCumulativeCountWhere asCumulativeCountWhere() {
        return Js.uncheckedCast(this);
    }

    @JsProperty
    String getType();
}
