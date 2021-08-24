package io.deephaven.javascript.proto.dhinternal.grpcweb.invoke;

import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(isNative = true, name = "dhinternal.grpcWeb.invoke.Request", namespace = JsPackage.GLOBAL)
public interface Request {
    @JsFunction
    public interface CloseFn {
        void onInvoke();
    }

    @JsOverlay
    static Request create() {
        return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    Request.CloseFn getClose();

    @JsProperty
    void setClose(Request.CloseFn close);
}
