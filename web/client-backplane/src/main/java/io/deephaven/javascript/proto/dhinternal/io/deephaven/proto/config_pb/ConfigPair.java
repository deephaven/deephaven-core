package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.config_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.config_pb.ConfigPair",
        namespace = JsPackage.GLOBAL)
public class ConfigPair {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static ConfigPair.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getKey();

        @JsProperty
        String getStringValue();

        @JsProperty
        void setKey(String key);

        @JsProperty
        void setStringValue(String stringValue);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static ConfigPair.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getKey();

        @JsProperty
        String getStringValue();

        @JsProperty
        void setKey(String key);

        @JsProperty
        void setStringValue(String stringValue);
    }

    public static native ConfigPair deserializeBinary(Uint8Array bytes);

    public static native ConfigPair deserializeBinaryFromReader(ConfigPair message, Object reader);

    public static native void serializeBinaryToWriter(ConfigPair message, Object writer);

    public static native ConfigPair.ToObjectReturnType toObject(
            boolean includeInstance, ConfigPair msg);

    public native void clearStringValue();

    public native String getKey();

    public native String getStringValue();

    public native int getValueCase();

    public native boolean hasStringValue();

    public native Uint8Array serializeBinary();

    public native void setKey(String value);

    public native void setStringValue(String value);

    public native ConfigPair.ToObjectReturnType0 toObject();

    public native ConfigPair.ToObjectReturnType0 toObject(boolean includeInstance);
}
