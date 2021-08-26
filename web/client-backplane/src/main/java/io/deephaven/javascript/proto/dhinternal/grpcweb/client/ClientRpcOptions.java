package io.deephaven.javascript.proto.dhinternal.grpcweb.client;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.grpcWeb.client.ClientRpcOptions",
        namespace = JsPackage.GLOBAL)
public interface ClientRpcOptions extends RpcOptions {
    @JsOverlay
    static ClientRpcOptions create() {
        return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    String getHost();

    @JsProperty
    void setHost(String host);
}
