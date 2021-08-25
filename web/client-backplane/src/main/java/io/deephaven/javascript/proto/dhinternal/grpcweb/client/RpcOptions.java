package io.deephaven.javascript.proto.dhinternal.grpcweb.client;

import io.deephaven.javascript.proto.dhinternal.grpcweb.transports.transport.TransportFactory;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.grpcWeb.client.RpcOptions",
        namespace = JsPackage.GLOBAL)
public interface RpcOptions {
    @JsOverlay
    static RpcOptions create() {
        return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    TransportFactory getTransport();

    @JsProperty
    boolean isDebug();

    @JsProperty
    void setDebug(boolean debug);

    @JsProperty
    void setTransport(TransportFactory transport);
}
