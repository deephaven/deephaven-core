package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.application_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.application_pb.ListFieldsRequest",
        namespace = JsPackage.GLOBAL)
public class ListFieldsRequest {
    public static native ListFieldsRequest deserializeBinary(Uint8Array bytes);

    public static native ListFieldsRequest deserializeBinaryFromReader(
            ListFieldsRequest message, Object reader);

    public static native void serializeBinaryToWriter(ListFieldsRequest message, Object writer);

    public static native Object toObject(boolean includeInstance, ListFieldsRequest msg);

    public native Uint8Array serializeBinary();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);
}
