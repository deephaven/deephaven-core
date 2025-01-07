//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn.updatebyspec;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.table_pb.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByFill",
        namespace = JsPackage.GLOBAL)
public class UpdateByFill {
    public static native UpdateByFill deserializeBinary(Uint8Array bytes);

    public static native UpdateByFill deserializeBinaryFromReader(
            UpdateByFill message, Object reader);

    public static native void serializeBinaryToWriter(UpdateByFill message, Object writer);

    public static native Object toObject(boolean includeInstance, UpdateByFill msg);

    public native Uint8Array serializeBinary();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);
}
