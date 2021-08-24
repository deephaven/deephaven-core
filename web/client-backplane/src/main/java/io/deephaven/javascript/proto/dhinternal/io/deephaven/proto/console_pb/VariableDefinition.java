package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.console_pb.VariableDefinition",
    namespace = JsPackage.GLOBAL)
public class VariableDefinition {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static VariableDefinition.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getName();

        @JsProperty
        String getType();

        @JsProperty
        void setName(String name);

        @JsProperty
        void setType(String type);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static VariableDefinition.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getName();

        @JsProperty
        String getType();

        @JsProperty
        void setName(String name);

        @JsProperty
        void setType(String type);
    }

    public static native VariableDefinition deserializeBinary(Uint8Array bytes);

    public static native VariableDefinition deserializeBinaryFromReader(
        VariableDefinition message, Object reader);

    public static native void serializeBinaryToWriter(VariableDefinition message, Object writer);

    public static native VariableDefinition.ToObjectReturnType toObject(
        boolean includeInstance, VariableDefinition msg);

    public native String getName();

    public native String getType();

    public native Uint8Array serializeBinary();

    public native void setName(String value);

    public native void setType(String value);

    public native VariableDefinition.ToObjectReturnType0 toObject();

    public native VariableDefinition.ToObjectReturnType0 toObject(boolean includeInstance);
}
