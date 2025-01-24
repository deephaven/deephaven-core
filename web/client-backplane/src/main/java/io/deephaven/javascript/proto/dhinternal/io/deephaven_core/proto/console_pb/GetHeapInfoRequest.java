//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.console_pb.GetHeapInfoRequest",
        namespace = JsPackage.GLOBAL)
public class GetHeapInfoRequest {
    public static native GetHeapInfoRequest deserializeBinary(Uint8Array bytes);

    public static native GetHeapInfoRequest deserializeBinaryFromReader(
            GetHeapInfoRequest message, Object reader);

    public static native void serializeBinaryToWriter(GetHeapInfoRequest message, Object writer);

    public static native Object toObject(boolean includeInstance, GetHeapInfoRequest msg);

    public native Uint8Array serializeBinary();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);
}
