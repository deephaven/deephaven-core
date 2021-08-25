package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.console_pb.FigureDescriptor.BoolMapWithDefault",
        namespace = JsPackage.GLOBAL)
public class BoolMapWithDefault {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static BoolMapWithDefault.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<String> getKeysList();

        @JsProperty
        JsArray<Boolean> getValuesList();

        @JsProperty
        boolean isDefaultBool();

        @JsProperty
        void setDefaultBool(boolean defaultBool);

        @JsProperty
        void setKeysList(JsArray<String> keysList);

        @JsOverlay
        default void setKeysList(String[] keysList) {
            setKeysList(Js.<JsArray<String>>uncheckedCast(keysList));
        }

        @JsProperty
        void setValuesList(JsArray<Boolean> valuesList);

        @JsOverlay
        default void setValuesList(boolean[] valuesList) {
            setValuesList(Js.<JsArray<Boolean>>uncheckedCast(valuesList));
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static BoolMapWithDefault.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<String> getKeysList();

        @JsProperty
        JsArray<Boolean> getValuesList();

        @JsProperty
        boolean isDefaultBool();

        @JsProperty
        void setDefaultBool(boolean defaultBool);

        @JsProperty
        void setKeysList(JsArray<String> keysList);

        @JsOverlay
        default void setKeysList(String[] keysList) {
            setKeysList(Js.<JsArray<String>>uncheckedCast(keysList));
        }

        @JsProperty
        void setValuesList(JsArray<Boolean> valuesList);

        @JsOverlay
        default void setValuesList(boolean[] valuesList) {
            setValuesList(Js.<JsArray<Boolean>>uncheckedCast(valuesList));
        }
    }

    public static native BoolMapWithDefault deserializeBinary(Uint8Array bytes);

    public static native BoolMapWithDefault deserializeBinaryFromReader(
            BoolMapWithDefault message, Object reader);

    public static native void serializeBinaryToWriter(BoolMapWithDefault message, Object writer);

    public static native BoolMapWithDefault.ToObjectReturnType toObject(
            boolean includeInstance, BoolMapWithDefault msg);

    public native String addKeys(String value, double index);

    public native String addKeys(String value);

    public native boolean addValues(boolean value, double index);

    public native boolean addValues(boolean value);

    public native void clearKeysList();

    public native void clearValuesList();

    public native boolean getDefaultBool();

    public native JsArray<String> getKeysList();

    public native JsArray<Boolean> getValuesList();

    public native Uint8Array serializeBinary();

    public native void setDefaultBool(boolean value);

    public native void setKeysList(JsArray<String> value);

    @JsOverlay
    public final void setKeysList(String[] value) {
        setKeysList(Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void setValuesList(JsArray<Boolean> value);

    @JsOverlay
    public final void setValuesList(boolean[] value) {
        setValuesList(Js.<JsArray<Boolean>>uncheckedCast(value));
    }

    public native BoolMapWithDefault.ToObjectReturnType0 toObject();

    public native BoolMapWithDefault.ToObjectReturnType0 toObject(boolean includeInstance);
}
