package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.console_pb.BindTableToVariableResponse",
    namespace = JsPackage.GLOBAL)
public class BindTableToVariableResponse {
    public static native BindTableToVariableResponse deserializeBinary(Uint8Array bytes);

    public static native BindTableToVariableResponse deserializeBinaryFromReader(
        BindTableToVariableResponse message, Object reader);

    public static native void serializeBinaryToWriter(
        BindTableToVariableResponse message, Object writer);

    public static native Object toObject(boolean includeInstance, BindTableToVariableResponse msg);

    public native Uint8Array serializeBinary();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);
}
