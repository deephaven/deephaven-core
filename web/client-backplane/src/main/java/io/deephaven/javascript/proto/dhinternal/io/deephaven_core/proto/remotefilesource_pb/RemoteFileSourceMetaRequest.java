//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.remotefilesource_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.remotefilesource_pb.RemoteFileSourceMetaRequest",
        namespace = JsPackage.GLOBAL)
public class RemoteFileSourceMetaRequest {
    public static native RemoteFileSourceMetaRequest deserializeBinary(Uint8Array bytes);

    public static native RemoteFileSourceMetaRequest deserializeBinaryFromReader(
            RemoteFileSourceMetaRequest message, Object reader);

    public static native void serializeBinaryToWriter(
            RemoteFileSourceMetaRequest message, Object writer);

    public native void clearResourceName();

    public native String getResourceName();

    public native Uint8Array serializeBinary();

    public native void setResourceName(String value);
}
