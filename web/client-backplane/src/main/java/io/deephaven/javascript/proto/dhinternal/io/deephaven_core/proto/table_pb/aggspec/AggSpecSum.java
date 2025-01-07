//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.aggspec;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.table_pb.AggSpec.AggSpecSum",
        namespace = JsPackage.GLOBAL)
public class AggSpecSum {
    public static native AggSpecSum deserializeBinary(Uint8Array bytes);

    public static native AggSpecSum deserializeBinaryFromReader(AggSpecSum message, Object reader);

    public static native void serializeBinaryToWriter(AggSpecSum message, Object writer);

    public static native Object toObject(boolean includeInstance, AggSpecSum msg);

    public native Uint8Array serializeBinary();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);
}
