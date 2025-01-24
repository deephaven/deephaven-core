//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.storage_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.storage_pb.CreateDirectoryResponse",
        namespace = JsPackage.GLOBAL)
public class CreateDirectoryResponse {
    public static native CreateDirectoryResponse deserializeBinary(Uint8Array bytes);

    public static native CreateDirectoryResponse deserializeBinaryFromReader(
            CreateDirectoryResponse message, Object reader);

    public static native void serializeBinaryToWriter(CreateDirectoryResponse message, Object writer);

    public static native Object toObject(boolean includeInstance, CreateDirectoryResponse msg);

    public native Uint8Array serializeBinary();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);
}
