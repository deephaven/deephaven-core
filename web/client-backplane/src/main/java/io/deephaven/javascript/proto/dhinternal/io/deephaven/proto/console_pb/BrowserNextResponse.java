package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.console_pb.BrowserNextResponse",
        namespace = JsPackage.GLOBAL)
public class BrowserNextResponse {
    public static native BrowserNextResponse deserializeBinary(Uint8Array bytes);

    public static native BrowserNextResponse deserializeBinaryFromReader(
            BrowserNextResponse message, Object reader);

    public static native void serializeBinaryToWriter(BrowserNextResponse message, Object writer);

    public static native Object toObject(boolean includeInstance, BrowserNextResponse msg);

    public native Uint8Array serializeBinary();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);
}
