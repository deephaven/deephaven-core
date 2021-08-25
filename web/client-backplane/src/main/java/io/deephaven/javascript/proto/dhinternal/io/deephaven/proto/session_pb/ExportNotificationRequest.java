package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.session_pb.ExportNotificationRequest",
    namespace = JsPackage.GLOBAL)
public class ExportNotificationRequest {
    public static native ExportNotificationRequest deserializeBinary(Uint8Array bytes);

    public static native ExportNotificationRequest deserializeBinaryFromReader(
        ExportNotificationRequest message, Object reader);

    public static native void serializeBinaryToWriter(
        ExportNotificationRequest message, Object writer);

    public static native Object toObject(boolean includeInstance, ExportNotificationRequest msg);

    public native Uint8Array serializeBinary();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);
}
