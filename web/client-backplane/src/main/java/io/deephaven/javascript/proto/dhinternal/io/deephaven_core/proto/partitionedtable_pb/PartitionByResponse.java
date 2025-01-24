//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.partitionedtable_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.partitionedtable_pb.PartitionByResponse",
        namespace = JsPackage.GLOBAL)
public class PartitionByResponse {
    public static native PartitionByResponse deserializeBinary(Uint8Array bytes);

    public static native PartitionByResponse deserializeBinaryFromReader(
            PartitionByResponse message, Object reader);

    public static native void serializeBinaryToWriter(PartitionByResponse message, Object writer);

    public static native Object toObject(boolean includeInstance, PartitionByResponse msg);

    public native Uint8Array serializeBinary();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);
}
