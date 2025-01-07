//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.config_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.config_pb.ConfigurationConstantsRequest",
        namespace = JsPackage.GLOBAL)
public class ConfigurationConstantsRequest {
    public static native ConfigurationConstantsRequest deserializeBinary(Uint8Array bytes);

    public static native ConfigurationConstantsRequest deserializeBinaryFromReader(
            ConfigurationConstantsRequest message, Object reader);

    public static native void serializeBinaryToWriter(
            ConfigurationConstantsRequest message, Object writer);

    public static native Object toObject(boolean includeInstance, ConfigurationConstantsRequest msg);

    public native Uint8Array serializeBinary();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);
}
