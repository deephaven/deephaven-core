//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.remotefilesource_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.remotefilesource_pb.SetExecutionContextResponse",
        namespace = JsPackage.GLOBAL)
public class SetExecutionContextResponse {
    public static native SetExecutionContextResponse deserializeBinary(Uint8Array bytes);

    public static native SetExecutionContextResponse deserializeBinaryFromReader(
            SetExecutionContextResponse message, Object reader);

    public static native void serializeBinaryToWriter(
            SetExecutionContextResponse message, Object writer);

    public native void clearSuccess();

    public native boolean getSuccess();

    public native Uint8Array serializeBinary();


    public native void setSuccess(boolean value);
}

