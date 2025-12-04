//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.remotefilesource_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.remotefilesource_pb.RemoteFileSourceMetaResponse",
        namespace = JsPackage.GLOBAL)
public class RemoteFileSourceMetaResponse {
    public static native RemoteFileSourceMetaResponse deserializeBinary(Uint8Array bytes);

    public static native RemoteFileSourceMetaResponse deserializeBinaryFromReader(
            RemoteFileSourceMetaResponse message, Object reader);

    public static native void serializeBinaryToWriter(
            RemoteFileSourceMetaResponse message, Object writer);

    public native void clearContent();

    public native void clearFound();

    public native void clearError();

    public native Uint8Array getContent();

    public native boolean getFound();

    public native String getError();

    public native Uint8Array serializeBinary();

    public native void setContent(Uint8Array value);

    public native void setFound(boolean value);

    public native void setError(String value);
}

