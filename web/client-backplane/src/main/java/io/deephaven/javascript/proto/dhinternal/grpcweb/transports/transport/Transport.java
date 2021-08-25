package io.deephaven.javascript.proto.dhinternal.grpcweb.transports.transport;

import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.browserheaders.BrowserHeaders;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name = "dhinternal.grpcWeb.transports.Transport.Transport",
    namespace = JsPackage.GLOBAL)
public interface Transport {
    void cancel();

    void finishSend();

    void sendMessage(Uint8Array msgBytes);

    void start(BrowserHeaders metadata);
}
