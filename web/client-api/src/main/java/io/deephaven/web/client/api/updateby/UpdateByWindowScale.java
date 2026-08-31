//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import com.vertispan.tsdefs.annotations.TsUnion;
import com.vertispan.tsdefs.annotations.TsUnionMember;
import io.deephaven.web.client.api.LongWrapper;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

@TsUnion
@JsType(name = "?", namespace = JsPackage.GLOBAL, isNative = true)
public interface UpdateByWindowScale {
    @TsUnionMember
    @JsOverlay
    default UpdateByWindowTicks asTicks() {
        return Js.uncheckedCast(this);
    }

    @TsUnionMember
    @JsOverlay
    default UpdateByWindowTime asTime() {
        return Js.uncheckedCast(this);
    }

    @JsProperty
    String getType();
}
