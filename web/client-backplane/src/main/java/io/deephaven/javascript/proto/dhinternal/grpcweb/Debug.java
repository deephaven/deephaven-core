package io.deephaven.javascript.proto.dhinternal.grpcweb;

import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(isNative = true, name = "dhinternal.grpcWeb.debug", namespace = JsPackage.GLOBAL)
public class Debug {
    public static native void debug(Object... args);
}
