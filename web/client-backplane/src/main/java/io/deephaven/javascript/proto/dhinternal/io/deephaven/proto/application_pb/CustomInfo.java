package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.application_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.application_pb.CustomInfo",
        namespace = JsPackage.GLOBAL)
public class CustomInfo {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static CustomInfo.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getType();

        @JsProperty
        void setType(String type);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static CustomInfo.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getType();

        @JsProperty
        void setType(String type);
    }

    public static native CustomInfo deserializeBinary(Uint8Array bytes);

    public static native CustomInfo deserializeBinaryFromReader(CustomInfo message, Object reader);

    public static native void serializeBinaryToWriter(CustomInfo message, Object writer);

    public static native CustomInfo.ToObjectReturnType toObject(
            boolean includeInstance, CustomInfo msg);

    public native String getType();

    public native Uint8Array serializeBinary();

    public native void setType(String value);

    public native CustomInfo.ToObjectReturnType0 toObject();

    public native CustomInfo.ToObjectReturnType0 toObject(boolean includeInstance);
}
