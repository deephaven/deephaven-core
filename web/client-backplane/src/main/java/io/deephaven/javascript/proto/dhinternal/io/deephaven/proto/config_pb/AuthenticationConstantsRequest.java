package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.config_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.config_pb.AuthenticationConstantsRequest",
        namespace = JsPackage.GLOBAL)
public class AuthenticationConstantsRequest {
    public static native AuthenticationConstantsRequest deserializeBinary(Uint8Array bytes);

    public static native AuthenticationConstantsRequest deserializeBinaryFromReader(
            AuthenticationConstantsRequest message, Object reader);

    public static native void serializeBinaryToWriter(
            AuthenticationConstantsRequest message, Object writer);

    public static native Object toObject(boolean includeInstance, AuthenticationConstantsRequest msg);

    public native Uint8Array serializeBinary();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);
}
