package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.hierarchicaltable_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.hierarchicaltable_pb.HierarchicalTableViewResponse",
        namespace = JsPackage.GLOBAL)
public class HierarchicalTableViewResponse {
    public static native HierarchicalTableViewResponse deserializeBinary(Uint8Array bytes);

    public static native HierarchicalTableViewResponse deserializeBinaryFromReader(
            HierarchicalTableViewResponse message, Object reader);

    public static native void serializeBinaryToWriter(
            HierarchicalTableViewResponse message, Object writer);

    public static native Object toObject(boolean includeInstance, HierarchicalTableViewResponse msg);

    public native Uint8Array serializeBinary();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);
}
