package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.aggspec;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.AggSpec.AggSpecAbsSum",
        namespace = JsPackage.GLOBAL)
public class AggSpecAbsSum {
    public static native AggSpecAbsSum deserializeBinary(Uint8Array bytes);

    public static native AggSpecAbsSum deserializeBinaryFromReader(
            AggSpecAbsSum message, Object reader);

    public static native void serializeBinaryToWriter(AggSpecAbsSum message, Object writer);

    public static native Object toObject(boolean includeInstance, AggSpecAbsSum msg);

    public native Uint8Array serializeBinary();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);
}
