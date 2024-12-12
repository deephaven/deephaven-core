//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.grpc;

import com.vertispan.tsdefs.annotations.TsUnion;
import com.vertispan.tsdefs.annotations.TsUnionMember;
import elemental2.core.JsArray;
import javaemul.internal.annotations.DoNotAutobox;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

/**
 * Union of string and array of string, as node/browser APIs tend to accept either for http headers.
 */
@TsUnion
@JsType(name = "?", namespace = JsPackage.GLOBAL, isNative = true)
public interface HeaderValueUnion {
    @JsOverlay
    static HeaderValueUnion of(@DoNotAutobox Object value) {
        return Js.cast(value);
    }

    @JsOverlay
    default boolean isArray() {
        return JsArray.isArray(this);
    }

    @TsUnionMember
    @JsOverlay
    default String asString() {
        return Js.cast(this);
    }

    @TsUnionMember
    @JsOverlay
    default JsArray<String> asArray() {
        return Js.cast(this);
    }
}
