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
        name = "dhinternal.io.deephaven.proto.console_pb.FigureDescriptor.StringMapWithDefault",
        namespace = JsPackage.GLOBAL)
public class StringMapWithDefault {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static StringMapWithDefault.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getDefaultString();

        @JsProperty
        JsArray<String> getKeysList();

        @JsProperty
        JsArray<String> getValuesList();

        @JsProperty
        void setDefaultString(String defaultString);

        @JsProperty
        void setKeysList(JsArray<String> keysList);

        @JsOverlay
        default void setKeysList(String[] keysList) {
            setKeysList(Js.<JsArray<String>>uncheckedCast(keysList));
        }

        @JsProperty
        void setValuesList(JsArray<String> valuesList);

        @JsOverlay
        default void setValuesList(String[] valuesList) {
            setValuesList(Js.<JsArray<String>>uncheckedCast(valuesList));
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static StringMapWithDefault.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getDefaultString();

        @JsProperty
        JsArray<String> getKeysList();

        @JsProperty
        JsArray<String> getValuesList();

        @JsProperty
        void setDefaultString(String defaultString);

        @JsProperty
        void setKeysList(JsArray<String> keysList);

        @JsOverlay
        default void setKeysList(String[] keysList) {
            setKeysList(Js.<JsArray<String>>uncheckedCast(keysList));
        }

        @JsProperty
        void setValuesList(JsArray<String> valuesList);

        @JsOverlay
        default void setValuesList(String[] valuesList) {
            setValuesList(Js.<JsArray<String>>uncheckedCast(valuesList));
        }
    }

    public static native StringMapWithDefault deserializeBinary(Uint8Array bytes);

    public static native StringMapWithDefault deserializeBinaryFromReader(
            StringMapWithDefault message, Object reader);

    public static native void serializeBinaryToWriter(StringMapWithDefault message, Object writer);

    public static native StringMapWithDefault.ToObjectReturnType toObject(
            boolean includeInstance, StringMapWithDefault msg);

    public native String addKeys(String value, double index);

    public native String addKeys(String value);

    public native String addValues(String value, double index);

    public native String addValues(String value);

    public native void clearKeysList();

    public native void clearValuesList();

    public native String getDefaultString();

    public native JsArray<String> getKeysList();

    public native JsArray<String> getValuesList();

    public native Uint8Array serializeBinary();

    public native void setDefaultString(String value);

    public native void setKeysList(JsArray<String> value);

    @JsOverlay
    public final void setKeysList(String[] value) {
        setKeysList(Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void setValuesList(JsArray<String> value);

    @JsOverlay
    public final void setValuesList(String[] value) {
        setValuesList(Js.<JsArray<String>>uncheckedCast(value));
    }

    public native StringMapWithDefault.ToObjectReturnType0 toObject();

    public native StringMapWithDefault.ToObjectReturnType0 toObject(boolean includeInstance);
}
