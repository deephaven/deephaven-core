package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.application_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.application_pb.FigureInfo",
        namespace = JsPackage.GLOBAL)
public class FigureInfo {
    public static native FigureInfo deserializeBinary(Uint8Array bytes);

    public static native FigureInfo deserializeBinaryFromReader(FigureInfo message, Object reader);

    public static native void serializeBinaryToWriter(FigureInfo message, Object writer);

    public static native Object toObject(boolean includeInstance, FigureInfo msg);

    public native Uint8Array serializeBinary();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);
}
