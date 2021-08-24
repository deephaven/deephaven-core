package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.console_pb.ChangeDocumentResponse",
    namespace = JsPackage.GLOBAL)
public class ChangeDocumentResponse {
    public static native ChangeDocumentResponse deserializeBinary(Uint8Array bytes);

    public static native ChangeDocumentResponse deserializeBinaryFromReader(
        ChangeDocumentResponse message, Object reader);

    public static native void serializeBinaryToWriter(ChangeDocumentResponse message,
        Object writer);

    public static native Object toObject(boolean includeInstance, ChangeDocumentResponse msg);

    public native Uint8Array serializeBinary();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);
}
