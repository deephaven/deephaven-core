//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.remotefilesource;

import com.vertispan.tsdefs.annotations.TsUnion;
import com.vertispan.tsdefs.annotations.TsUnionMember;
import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

/**
 * Union type for resource content that can be either a String or Uint8Array.
 */
@TsUnion
@JsType(name = "?", namespace = JsPackage.GLOBAL, isNative = true)
public interface ResourceContentUnion {
    @JsOverlay
    static ResourceContentUnion of(Object o) {
        return Js.cast(o);
    }

    @JsOverlay
    default boolean isString() {
        // Cast to (Object) since Java only "knows" that `this` is `ResourceContentUnion` type which cannot have a
        // subclass that is also a String.
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

