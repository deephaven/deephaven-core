package io.deephaven.javascript.proto.dhinternal.grpcweb.transports.http.xhr;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.grpcWeb.transports.http.xhr.XhrTransportInit",
    namespace = JsPackage.GLOBAL)
public interface XhrTransportInit {
    @JsOverlay
    static XhrTransportInit create() {
        return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    boolean isWithCredentials();

    @JsProperty
    void setWithCredentials(boolean withCredentials);
}
