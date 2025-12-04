//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.remotefilesource_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.remotefilesource_pb.RemoteFileSourceSetConnectionIdResponse",
        namespace = JsPackage.GLOBAL)
public class RemoteFileSourceSetConnectionIdResponse {
    public static native RemoteFileSourceSetConnectionIdResponse deserializeBinary(Uint8Array bytes);

    public static native RemoteFileSourceSetConnectionIdResponse deserializeBinaryFromReader(
            RemoteFileSourceSetConnectionIdResponse message, Object reader);

    public static native void serializeBinaryToWriter(
            RemoteFileSourceSetConnectionIdResponse message, Object writer);

    public native void clearConnectionId();

    public native void clearSuccess();

    public native String getConnectionId();

    public native boolean getSuccess();

    public native Uint8Array serializeBinary();

    public native void setConnectionId(String value);

    public native void setSuccess(boolean value);
}

