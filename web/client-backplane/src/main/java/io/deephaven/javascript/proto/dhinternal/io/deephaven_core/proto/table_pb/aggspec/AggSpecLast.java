//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.aggspec;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.table_pb.AggSpec.AggSpecLast",
        namespace = JsPackage.GLOBAL)
public class AggSpecLast {
    public static native AggSpecLast deserializeBinary(Uint8Array bytes);

    public static native AggSpecLast deserializeBinaryFromReader(AggSpecLast message, Object reader);

    public static native void serializeBinaryToWriter(AggSpecLast message, Object writer);

    public static native Object toObject(boolean includeInstance, AggSpecLast msg);

    public native Uint8Array serializeBinary();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);
}
