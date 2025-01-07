//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.config_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.config_pb.ConfigValue",
        namespace = JsPackage.GLOBAL)
public class ConfigValue {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static ConfigValue.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getStringValue();

        @JsProperty
        void setStringValue(String stringValue);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static ConfigValue.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getStringValue();

        @JsProperty
        void setStringValue(String stringValue);
    }

    public static native ConfigValue deserializeBinary(Uint8Array bytes);

    public static native ConfigValue deserializeBinaryFromReader(ConfigValue message, Object reader);

    public static native void serializeBinaryToWriter(ConfigValue message, Object writer);

    public static native ConfigValue.ToObjectReturnType toObject(
            boolean includeInstance, ConfigValue msg);

    public native void clearStringValue();

    public native int getKindCase();

    public native String getStringValue();

    public native boolean hasStringValue();

    public native Uint8Array serializeBinary();

    public native void setStringValue(String value);

    public native ConfigValue.ToObjectReturnType0 toObject();

    public native ConfigValue.ToObjectReturnType0 toObject(boolean includeInstance);
}
