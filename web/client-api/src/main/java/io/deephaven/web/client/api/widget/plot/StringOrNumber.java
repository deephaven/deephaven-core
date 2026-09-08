//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.widget.plot;

import com.vertispan.tsdefs.annotations.TsUnion;
import com.vertispan.tsdefs.annotations.TsUnionMember;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

@TsUnion
@JsType(name = "?", namespace = JsPackage.GLOBAL, isNative = true)
public interface StringOrNumber {
    @JsOverlay
    @TsUnionMember
    default String asString() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    @TsUnionMember
    default Double asNumber() {
        return Js.uncheckedCast(this);
    }

    @JsOverlay
    default int coerceToInt() {
        return Js.coerceToInt(this);
    }
}
