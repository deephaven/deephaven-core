//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn.updatebyspec;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByCumulativeProduct",
        namespace = JsPackage.GLOBAL)
public class UpdateByCumulativeProduct {
    public static native UpdateByCumulativeProduct deserializeBinary(Uint8Array bytes);

    public static native UpdateByCumulativeProduct deserializeBinaryFromReader(
            UpdateByCumulativeProduct message, Object reader);

    public static native void serializeBinaryToWriter(
            UpdateByCumulativeProduct message, Object writer);

    public static native Object toObject(boolean includeInstance, UpdateByCumulativeProduct msg);

    public native Uint8Array serializeBinary();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);
}
