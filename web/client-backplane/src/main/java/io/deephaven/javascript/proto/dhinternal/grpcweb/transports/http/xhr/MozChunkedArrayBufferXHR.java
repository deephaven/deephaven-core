package io.deephaven.javascript.proto.dhinternal.grpcweb.transports.http.xhr;

import io.deephaven.javascript.proto.dhinternal.grpcweb.transports.transport.TransportOptions;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name = "dhinternal.grpcWeb.transports.http.xhr.MozChunkedArrayBufferXHR",
    namespace = JsPackage.GLOBAL)
public class MozChunkedArrayBufferXHR extends XHR {
    public MozChunkedArrayBufferXHR() {
        // This super call is here only for the code to compile; it is never executed.
        super((TransportOptions) null, (XhrTransportInit) null);
    }

    public native void configureXhr();

    public native void onProgressEvent();
}
