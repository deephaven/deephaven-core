package io.deephaven.javascript.proto.dhinternal.grpcweb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(isNative = true, name = "dhinternal.grpcWeb.ChunkParser", namespace = JsPackage.GLOBAL)
public class ChunkParser {
    public static native String decodeASCII(Uint8Array input);

    public static native Uint8Array encodeASCII(String input);
}
