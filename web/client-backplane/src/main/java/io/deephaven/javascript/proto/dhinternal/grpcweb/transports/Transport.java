package io.deephaven.javascript.proto.dhinternal.grpcweb.transports;

import io.deephaven.javascript.proto.dhinternal.grpcweb.transports.transport.TransportFactory;
import io.deephaven.javascript.proto.dhinternal.grpcweb.transports.transport.TransportOptions;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name = "dhinternal.grpcWeb.transports.Transport",
    namespace = JsPackage.GLOBAL)
public class Transport {
    public static native io.deephaven.javascript.proto.dhinternal.grpcweb.transports.transport.Transport makeDefaultTransport(
        TransportOptions options);

    public static native void setDefaultTransportFactory(TransportFactory t);
}
