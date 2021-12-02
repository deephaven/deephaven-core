package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.inputtable_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.inputtable_pb.AddTableResponse",
        namespace = JsPackage.GLOBAL)
public class AddTableResponse {
    public static native AddTableResponse deserializeBinary(Uint8Array bytes);

    public static native AddTableResponse deserializeBinaryFromReader(
            AddTableResponse message, Object reader);

    public static native void serializeBinaryToWriter(AddTableResponse message, Object writer);

    public static native Object toObject(boolean includeInstance, AddTableResponse msg);

    public native Uint8Array serializeBinary();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);
}
