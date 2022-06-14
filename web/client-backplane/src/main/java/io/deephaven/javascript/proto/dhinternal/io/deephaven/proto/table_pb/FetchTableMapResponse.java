/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.FetchTableMapResponse",
        namespace = JsPackage.GLOBAL)
public class FetchTableMapResponse {
    public static native FetchTableMapResponse deserializeBinary(Uint8Array bytes);

    public static native FetchTableMapResponse deserializeBinaryFromReader(
            FetchTableMapResponse message, Object reader);

    public static native void serializeBinaryToWriter(FetchTableMapResponse message, Object writer);

    public static native Object toObject(boolean includeInstance, FetchTableMapResponse msg);

    public native Uint8Array serializeBinary();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);
}
