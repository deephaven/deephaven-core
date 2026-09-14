//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import com.vertispan.tsdefs.annotations.TsUnion;
import com.vertispan.tsdefs.annotations.TsUnionMember;
import io.deephaven.web.client.api.LongWrapper;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

@TsUnion
@JsType(name = "?", namespace = JsPackage.GLOBAL, isNative = true)
public interface WindowDurationUnion {
    @JsOverlay
    @TsUnionMember
    default double asNanosNumber() {
        return Js.coerceToDouble(this);
    }

    @JsOverlay
    @TsUnionMember
    default String asDurationString() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default LongWrapper asNanosLong() {
        return Js.uncheckedCast(this);
    }
}
