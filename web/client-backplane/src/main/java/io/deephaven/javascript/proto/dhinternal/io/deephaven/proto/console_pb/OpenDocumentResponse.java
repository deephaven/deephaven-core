package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.console_pb.OpenDocumentResponse",
    namespace = JsPackage.GLOBAL)
public class OpenDocumentResponse {
    public static native OpenDocumentResponse deserializeBinary(Uint8Array bytes);

    public static native OpenDocumentResponse deserializeBinaryFromReader(
        OpenDocumentResponse message, Object reader);

    public static native void serializeBinaryToWriter(OpenDocumentResponse message, Object writer);

    public static native Object toObject(boolean includeInstance, OpenDocumentResponse msg);

    public native Uint8Array serializeBinary();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);
}
