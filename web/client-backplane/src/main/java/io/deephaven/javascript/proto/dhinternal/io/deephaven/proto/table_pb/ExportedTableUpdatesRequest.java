package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.ExportedTableUpdatesRequest",
        namespace = JsPackage.GLOBAL)
public class ExportedTableUpdatesRequest {
    public static native ExportedTableUpdatesRequest deserializeBinary(Uint8Array bytes);

    public static native ExportedTableUpdatesRequest deserializeBinaryFromReader(
            ExportedTableUpdatesRequest message, Object reader);

    public static native void serializeBinaryToWriter(
            ExportedTableUpdatesRequest message, Object writer);

    public static native Object toObject(boolean includeInstance, ExportedTableUpdatesRequest msg);

    public native Uint8Array serializeBinary();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);
}
