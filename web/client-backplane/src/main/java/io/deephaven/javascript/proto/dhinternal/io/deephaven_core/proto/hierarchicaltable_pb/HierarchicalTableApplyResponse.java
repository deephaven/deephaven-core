//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.hierarchicaltable_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.hierarchicaltable_pb.HierarchicalTableApplyResponse",
        namespace = JsPackage.GLOBAL)
public class HierarchicalTableApplyResponse {
    public static native HierarchicalTableApplyResponse deserializeBinary(Uint8Array bytes);

    public static native HierarchicalTableApplyResponse deserializeBinaryFromReader(
            HierarchicalTableApplyResponse message, Object reader);

    public static native void serializeBinaryToWriter(
            HierarchicalTableApplyResponse message, Object writer);

    public static native Object toObject(boolean includeInstance, HierarchicalTableApplyResponse msg);

    public native Uint8Array serializeBinary();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);
}
