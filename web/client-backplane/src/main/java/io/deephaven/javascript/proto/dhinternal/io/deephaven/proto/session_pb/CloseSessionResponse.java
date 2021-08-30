package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.session_pb.CloseSessionResponse",
        namespace = JsPackage.GLOBAL)
public class CloseSessionResponse {
    public static native CloseSessionResponse deserializeBinary(Uint8Array bytes);

    public static native CloseSessionResponse deserializeBinaryFromReader(
            CloseSessionResponse message, Object reader);

    public static native void serializeBinaryToWriter(CloseSessionResponse message, Object writer);

    public static native Object toObject(boolean includeInstance, CloseSessionResponse msg);

    public native Uint8Array serializeBinary();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);
}
