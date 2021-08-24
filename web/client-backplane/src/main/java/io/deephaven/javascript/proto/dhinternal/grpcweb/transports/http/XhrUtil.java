package io.deephaven.javascript.proto.dhinternal.grpcweb.transports.http;

import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name = "dhinternal.grpcWeb.transports.http.xhrUtil",
    namespace = JsPackage.GLOBAL)
public class XhrUtil {
    public static native boolean detectMozXHRSupport();

    public static native boolean detectXHROverrideMimeTypeSupport();

    public static native boolean xhrSupportsResponseType(String type);
}
