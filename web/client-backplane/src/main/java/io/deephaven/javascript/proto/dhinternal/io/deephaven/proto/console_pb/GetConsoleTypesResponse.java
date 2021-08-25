package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb;

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
        name = "dhinternal.io.deephaven.proto.console_pb.GetConsoleTypesResponse",
        namespace = JsPackage.GLOBAL)
public class GetConsoleTypesResponse {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static GetConsoleTypesResponse.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<String> getConsoleTypesList();

        @JsProperty
        void setConsoleTypesList(JsArray<String> consoleTypesList);

        @JsOverlay
        default void setConsoleTypesList(String[] consoleTypesList) {
            setConsoleTypesList(Js.<JsArray<String>>uncheckedCast(consoleTypesList));
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static GetConsoleTypesResponse.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<String> getConsoleTypesList();

        @JsProperty
        void setConsoleTypesList(JsArray<String> consoleTypesList);

        @JsOverlay
        default void setConsoleTypesList(String[] consoleTypesList) {
            setConsoleTypesList(Js.<JsArray<String>>uncheckedCast(consoleTypesList));
        }
    }

    public static native GetConsoleTypesResponse deserializeBinary(Uint8Array bytes);

    public static native GetConsoleTypesResponse deserializeBinaryFromReader(
            GetConsoleTypesResponse message, Object reader);

    public static native void serializeBinaryToWriter(GetConsoleTypesResponse message, Object writer);

    public static native GetConsoleTypesResponse.ToObjectReturnType toObject(
            boolean includeInstance, GetConsoleTypesResponse msg);

    public native String addConsoleTypes(String value, double index);

    public native String addConsoleTypes(String value);

    public native void clearConsoleTypesList();

    public native JsArray<String> getConsoleTypesList();

    public native Uint8Array serializeBinary();

    public native void setConsoleTypesList(JsArray<String> value);

    @JsOverlay
    public final void setConsoleTypesList(String[] value) {
        setConsoleTypesList(Js.<JsArray<String>>uncheckedCast(value));
    }

    public native GetConsoleTypesResponse.ToObjectReturnType0 toObject();

    public native GetConsoleTypesResponse.ToObjectReturnType0 toObject(boolean includeInstance);
}
