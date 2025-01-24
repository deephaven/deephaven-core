//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.session_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.session_pb.TerminationNotificationRequest",
        namespace = JsPackage.GLOBAL)
public class TerminationNotificationRequest {
    public static native TerminationNotificationRequest deserializeBinary(Uint8Array bytes);

    public static native TerminationNotificationRequest deserializeBinaryFromReader(
            TerminationNotificationRequest message, Object reader);

    public static native void serializeBinaryToWriter(
            TerminationNotificationRequest message, Object writer);

    public static native Object toObject(boolean includeInstance, TerminationNotificationRequest msg);

    public native Uint8Array serializeBinary();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);
}
