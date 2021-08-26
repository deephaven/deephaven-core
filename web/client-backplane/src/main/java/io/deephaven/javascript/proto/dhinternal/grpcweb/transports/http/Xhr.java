package io.deephaven.javascript.proto.dhinternal.grpcweb.transports.http;

import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.grpcweb.transports.http.xhr.XhrTransportInit;
import io.deephaven.javascript.proto.dhinternal.grpcweb.transports.transport.TransportFactory;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.grpcWeb.transports.http.xhr",
        namespace = JsPackage.GLOBAL)
public class Xhr {
    public static native TransportFactory XhrTransport(XhrTransportInit init);

    public static native Uint8Array stringToArrayBuffer(String str);
}
