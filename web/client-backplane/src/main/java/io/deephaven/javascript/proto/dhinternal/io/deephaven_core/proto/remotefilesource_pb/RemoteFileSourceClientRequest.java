//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.remotefilesource_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.remotefilesource_pb.RemoteFileSourceClientRequest",
        namespace = JsPackage.GLOBAL)
public class RemoteFileSourceClientRequest {
    public static native RemoteFileSourceClientRequest deserializeBinary(Uint8Array bytes);

    public static native RemoteFileSourceClientRequest deserializeBinaryFromReader(
            RemoteFileSourceClientRequest message, Object reader);

    public static native void serializeBinaryToWriter(
            RemoteFileSourceClientRequest message, Object writer);

    public native void clearRequestId();

    public native void clearMetaResponse();

    public native void clearTestCommand();

    public native String getRequestId();

    public native RemoteFileSourceMetaResponse getMetaResponse();

    public native String getTestCommand();

    public native boolean hasMetaResponse();

    public native boolean hasTestCommand();

    public native Uint8Array serializeBinary();

    public native void setRequestId(String value);

    public native void setMetaResponse();

    public native void setMetaResponse(RemoteFileSourceMetaResponse value);


    public native void setTestCommand(String value);
}

