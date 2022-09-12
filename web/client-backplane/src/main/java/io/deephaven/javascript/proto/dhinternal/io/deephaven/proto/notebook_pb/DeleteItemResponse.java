package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.notebook_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.notebook_pb.DeleteItemResponse",
        namespace = JsPackage.GLOBAL)
public class DeleteItemResponse {
    public static native DeleteItemResponse deserializeBinary(Uint8Array bytes);

    public static native DeleteItemResponse deserializeBinaryFromReader(
            DeleteItemResponse message, Object reader);

    public static native void serializeBinaryToWriter(DeleteItemResponse message, Object writer);

    public static native Object toObject(boolean includeInstance, DeleteItemResponse msg);

    public native Uint8Array serializeBinary();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);
}
