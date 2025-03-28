//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.ide;

import com.vertispan.tsdefs.annotations.TsUnion;
import com.vertispan.tsdefs.annotations.TsUnionMember;
import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

@TsUnion
@JsType(name = "?", namespace = JsPackage.GLOBAL, isNative = true)
public interface SharedExportBytesUnion {
    @JsOverlay
    static SharedExportBytesUnion of(Object o) {
        return Js.cast(o);
    }

    @JsOverlay
    default boolean isString() {
        return (Object) this instanceof String;
    }

    @JsOverlay
    default boolean isUint8Array() {
        return this instanceof Uint8Array;
    }

    @TsUnionMember
    @JsOverlay
    default String asString() {
        return Js.cast(this);
    }

    @TsUnionMember
    @JsOverlay
    default Uint8Array asUint8Array() {
        return (Uint8Array) this;
    }
}
