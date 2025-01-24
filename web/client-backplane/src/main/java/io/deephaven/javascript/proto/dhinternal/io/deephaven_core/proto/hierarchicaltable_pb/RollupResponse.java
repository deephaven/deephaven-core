//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.hierarchicaltable_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.hierarchicaltable_pb.RollupResponse",
        namespace = JsPackage.GLOBAL)
public class RollupResponse {
    public static native RollupResponse deserializeBinary(Uint8Array bytes);

    public static native RollupResponse deserializeBinaryFromReader(
            RollupResponse message, Object reader);

    public static native void serializeBinaryToWriter(RollupResponse message, Object writer);

    public static native Object toObject(boolean includeInstance, RollupResponse msg);

    public native Uint8Array serializeBinary();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);
}
