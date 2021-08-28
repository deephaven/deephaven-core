package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.application_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.application_pb.RemovedField",
        namespace = JsPackage.GLOBAL)
public class RemovedField {
    public static native RemovedField deserializeBinary(Uint8Array bytes);

    public static native RemovedField deserializeBinaryFromReader(
            RemovedField message, Object reader);

    public static native void serializeBinaryToWriter(RemovedField message, Object writer);

    public static native Object toObject(boolean includeInstance, RemovedField msg);

    public native Uint8Array serializeBinary();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);
}
