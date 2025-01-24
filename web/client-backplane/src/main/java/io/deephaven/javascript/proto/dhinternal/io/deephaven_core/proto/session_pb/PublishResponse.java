//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.session_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.session_pb.PublishResponse",
        namespace = JsPackage.GLOBAL)
public class PublishResponse {
    public static native PublishResponse deserializeBinary(Uint8Array bytes);

    public static native PublishResponse deserializeBinaryFromReader(
            PublishResponse message, Object reader);

    public static native void serializeBinaryToWriter(PublishResponse message, Object writer);

    public static native Object toObject(boolean includeInstance, PublishResponse msg);

    public native Uint8Array serializeBinary();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);
}
