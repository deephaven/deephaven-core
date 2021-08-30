package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.session_pb.ExportResponse",
        namespace = JsPackage.GLOBAL)
public class ExportResponse {
    public static native ExportResponse deserializeBinary(Uint8Array bytes);

    public static native ExportResponse deserializeBinaryFromReader(
            ExportResponse message, Object reader);

    public static native void serializeBinaryToWriter(ExportResponse message, Object writer);

    public static native Object toObject(boolean includeInstance, ExportResponse msg);

    public native Uint8Array serializeBinary();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);
}
