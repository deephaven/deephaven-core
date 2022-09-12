package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.notebook_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.notebook_pb.SaveFileResponse",
        namespace = JsPackage.GLOBAL)
public class SaveFileResponse {
    public static native SaveFileResponse deserializeBinary(Uint8Array bytes);

    public static native SaveFileResponse deserializeBinaryFromReader(
            SaveFileResponse message, Object reader);

    public static native void serializeBinaryToWriter(SaveFileResponse message, Object writer);

    public static native Object toObject(boolean includeInstance, SaveFileResponse msg);

    public native Uint8Array serializeBinary();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);
}
