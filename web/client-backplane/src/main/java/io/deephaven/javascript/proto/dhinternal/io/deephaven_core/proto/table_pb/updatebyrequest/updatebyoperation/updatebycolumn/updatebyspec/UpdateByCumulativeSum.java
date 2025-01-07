//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn.updatebyspec;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.table_pb.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByCumulativeSum",
        namespace = JsPackage.GLOBAL)
public class UpdateByCumulativeSum {
    public static native UpdateByCumulativeSum deserializeBinary(Uint8Array bytes);

    public static native UpdateByCumulativeSum deserializeBinaryFromReader(
            UpdateByCumulativeSum message, Object reader);

    public static native void serializeBinaryToWriter(UpdateByCumulativeSum message, Object writer);

    public static native Object toObject(boolean includeInstance, UpdateByCumulativeSum msg);

    public native Uint8Array serializeBinary();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);
}
