//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.console_pb.CancelCommandResponse",
        namespace = JsPackage.GLOBAL)
public class CancelCommandResponse {
    public static native CancelCommandResponse deserializeBinary(Uint8Array bytes);

    public static native CancelCommandResponse deserializeBinaryFromReader(
            CancelCommandResponse message, Object reader);

    public static native void serializeBinaryToWriter(CancelCommandResponse message, Object writer);

    public static native Object toObject(boolean includeInstance, CancelCommandResponse msg);

    public native Uint8Array serializeBinary();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);
}
