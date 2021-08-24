package io.deephaven.javascript.proto.dhinternal.grpcweb.service;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.grpcWeb.service.UnaryMethodDefinition",
    namespace = JsPackage.GLOBAL)
public interface UnaryMethodDefinition<TRequest, TResponse> extends MethodDefinition {
    @JsOverlay
    static UnaryMethodDefinition create() {
        return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    boolean isRequestStream();

    @JsProperty
    boolean isResponseStream();

    @JsProperty
    void setRequestStream(boolean requestStream);

    @JsProperty
    void setResponseStream(boolean responseStream);
}
