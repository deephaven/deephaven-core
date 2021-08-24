package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.console_pb.CloseDocumentResponse",
    namespace = JsPackage.GLOBAL)
public class CloseDocumentResponse {
    public static native CloseDocumentResponse deserializeBinary(Uint8Array bytes);

    public static native CloseDocumentResponse deserializeBinaryFromReader(
        CloseDocumentResponse message, Object reader);

    public static native void serializeBinaryToWriter(CloseDocumentResponse message, Object writer);

    public static native Object toObject(boolean includeInstance, CloseDocumentResponse msg);

    public native Uint8Array serializeBinary();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);
}
