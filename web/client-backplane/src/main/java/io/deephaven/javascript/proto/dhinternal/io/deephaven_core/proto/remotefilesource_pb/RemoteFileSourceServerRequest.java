//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.remotefilesource_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.remotefilesource_pb.RemoteFileSourceServerRequest",
        namespace = JsPackage.GLOBAL)
public class RemoteFileSourceServerRequest {
    public static native RemoteFileSourceServerRequest deserializeBinary(Uint8Array bytes);

    public static native RemoteFileSourceServerRequest deserializeBinaryFromReader(
            RemoteFileSourceServerRequest message, Object reader);

    public static native void serializeBinaryToWriter(
            RemoteFileSourceServerRequest message, Object writer);

    public native void clearRequestId();

    public native void clearMetaRequest();

    public native String getRequestId();

    public native RemoteFileSourceMetaRequest getMetaRequest();

    public native boolean hasMetaRequest();

    public native Uint8Array serializeBinary();

    public native void setRequestId(String value);

    public native void setMetaRequest();

    public native void setMetaRequest(RemoteFileSourceMetaRequest value);
}

