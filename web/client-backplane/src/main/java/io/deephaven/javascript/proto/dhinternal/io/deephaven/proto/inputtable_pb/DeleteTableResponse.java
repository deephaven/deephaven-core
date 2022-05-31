package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.inputtable_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.inputtable_pb.DeleteTableResponse",
        namespace = JsPackage.GLOBAL)
public class DeleteTableResponse {
    public static native DeleteTableResponse deserializeBinary(Uint8Array bytes);

    public static native DeleteTableResponse deserializeBinaryFromReader(
            DeleteTableResponse message, Object reader);

    public static native void serializeBinaryToWriter(DeleteTableResponse message, Object writer);

    public static native Object toObject(boolean includeInstance, DeleteTableResponse msg);

    public native Uint8Array serializeBinary();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);
}
