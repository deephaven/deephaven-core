package io.deephaven.javascript.proto.dhinternal.grpcweb;

import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.grpcweb.message.ProtobufMessage;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(isNative = true, name = "dhinternal.grpcWeb.util", namespace = JsPackage.GLOBAL)
public class Util {
    public static native Uint8Array frameRequest(ProtobufMessage request);
}
