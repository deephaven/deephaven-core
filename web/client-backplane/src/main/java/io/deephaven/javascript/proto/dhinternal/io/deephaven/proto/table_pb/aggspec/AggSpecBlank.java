package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.aggspec;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.AggSpec.AggSpecBlank",
        namespace = JsPackage.GLOBAL)
public class AggSpecBlank {
    public static native AggSpecBlank deserializeBinary(Uint8Array bytes);

    public static native AggSpecBlank deserializeBinaryFromReader(
            AggSpecBlank message, Object reader);

    public static native void serializeBinaryToWriter(AggSpecBlank message, Object writer);

    public static native Object toObject(boolean includeInstance, AggSpecBlank msg);

    public native Uint8Array serializeBinary();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);
}
