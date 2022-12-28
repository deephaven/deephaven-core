package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.aggspec;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.AggSpec.AggSpecStd",
        namespace = JsPackage.GLOBAL)
public class AggSpecStd {
    public static native AggSpecStd deserializeBinary(Uint8Array bytes);

    public static native AggSpecStd deserializeBinaryFromReader(AggSpecStd message, Object reader);

    public static native void serializeBinaryToWriter(AggSpecStd message, Object writer);

    public static native Object toObject(boolean includeInstance, AggSpecStd msg);

    public native Uint8Array serializeBinary();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);
}
