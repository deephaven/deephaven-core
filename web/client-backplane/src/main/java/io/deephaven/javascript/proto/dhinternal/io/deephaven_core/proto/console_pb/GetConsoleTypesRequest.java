//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.console_pb.GetConsoleTypesRequest",
        namespace = JsPackage.GLOBAL)
public class GetConsoleTypesRequest {
    public static native GetConsoleTypesRequest deserializeBinary(Uint8Array bytes);

    public static native GetConsoleTypesRequest deserializeBinaryFromReader(
            GetConsoleTypesRequest message, Object reader);

    public static native void serializeBinaryToWriter(GetConsoleTypesRequest message, Object writer);

    public static native Object toObject(boolean includeInstance, GetConsoleTypesRequest msg);

    public native Uint8Array serializeBinary();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);
}
