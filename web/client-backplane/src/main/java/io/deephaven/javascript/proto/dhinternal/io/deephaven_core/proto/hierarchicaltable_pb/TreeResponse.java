//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.hierarchicaltable_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.hierarchicaltable_pb.TreeResponse",
        namespace = JsPackage.GLOBAL)
public class TreeResponse {
    public static native TreeResponse deserializeBinary(Uint8Array bytes);

    public static native TreeResponse deserializeBinaryFromReader(
            TreeResponse message, Object reader);

    public static native void serializeBinaryToWriter(TreeResponse message, Object writer);

    public static native Object toObject(boolean includeInstance, TreeResponse msg);

    public native Uint8Array serializeBinary();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);
}
