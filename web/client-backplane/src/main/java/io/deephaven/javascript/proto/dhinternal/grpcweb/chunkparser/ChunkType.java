package io.deephaven.javascript.proto.dhinternal.grpcweb.chunkparser;

import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.grpcWeb.ChunkParser.ChunkType",
        namespace = JsPackage.GLOBAL)
public class ChunkType {
    public static int MESSAGE,
            TRAILERS;
}
