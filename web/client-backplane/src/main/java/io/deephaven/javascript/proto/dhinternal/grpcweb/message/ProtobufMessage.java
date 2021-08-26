package io.deephaven.javascript.proto.dhinternal.grpcweb.message;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.grpcWeb.message.ProtobufMessage",
        namespace = JsPackage.GLOBAL)
public interface ProtobufMessage {
    Uint8Array serializeBinary();

    Object toObject();
}
