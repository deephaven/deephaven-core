//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.session_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.session_pb.ReleaseResponse",
        namespace = JsPackage.GLOBAL)
public class ReleaseResponse {
    public static native ReleaseResponse deserializeBinary(Uint8Array bytes);

    public static native ReleaseResponse deserializeBinaryFromReader(
            ReleaseResponse message, Object reader);

    public static native void serializeBinaryToWriter(ReleaseResponse message, Object writer);

    public static native Object toObject(boolean includeInstance, ReleaseResponse msg);

    public native Uint8Array serializeBinary();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);
}
