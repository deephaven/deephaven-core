package io.deephaven.javascript.proto.dhinternal.grpcweb.transports.http.xhr;

import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.browserheaders.BrowserHeaders;
import io.deephaven.javascript.proto.dhinternal.grpcweb.transports.transport.Transport;
import io.deephaven.javascript.proto.dhinternal.grpcweb.transports.transport.TransportOptions;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name = "dhinternal.grpcWeb.transports.http.xhr.XHR",
    namespace = JsPackage.GLOBAL)
public class XHR implements Transport {
    public double index;
    public XhrTransportInit init;
    public BrowserHeaders metadata;
    public TransportOptions options;
    public Object xhr;

    public XHR(TransportOptions transportOptions, XhrTransportInit init) {}

    public native void cancel();

    public native void configureXhr();

    public native void finishSend();

    public native void onLoadEvent();

    public native void onProgressEvent();

    public native void onStateChange();

    public native void sendMessage(Uint8Array msgBytes);

    public native void start(BrowserHeaders metadata);
}
