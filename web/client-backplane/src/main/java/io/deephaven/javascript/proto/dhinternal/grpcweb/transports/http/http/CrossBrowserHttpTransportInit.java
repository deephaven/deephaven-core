package io.deephaven.javascript.proto.dhinternal.grpcweb.transports.http.http;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.grpcWeb.transports.http.http.CrossBrowserHttpTransportInit",
        namespace = JsPackage.GLOBAL)
public interface CrossBrowserHttpTransportInit {
    @JsOverlay
    static CrossBrowserHttpTransportInit create() {
        return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    boolean isWithCredentials();

    @JsProperty
    void setWithCredentials(boolean withCredentials);
}
