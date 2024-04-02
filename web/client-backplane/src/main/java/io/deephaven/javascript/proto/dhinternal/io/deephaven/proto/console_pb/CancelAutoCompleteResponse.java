//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.console_pb.CancelAutoCompleteResponse",
        namespace = JsPackage.GLOBAL)
public class CancelAutoCompleteResponse {
    public static native CancelAutoCompleteResponse deserializeBinary(Uint8Array bytes);

    public static native CancelAutoCompleteResponse deserializeBinaryFromReader(
            CancelAutoCompleteResponse message, Object reader);

    public static native void serializeBinaryToWriter(
            CancelAutoCompleteResponse message, Object writer);

    public static native Object toObject(boolean includeInstance, CancelAutoCompleteResponse msg);

    public native Uint8Array serializeBinary();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);
}
