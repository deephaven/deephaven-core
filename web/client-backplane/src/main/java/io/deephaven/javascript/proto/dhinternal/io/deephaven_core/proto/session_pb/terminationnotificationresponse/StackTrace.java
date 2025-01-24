//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.session_pb.terminationnotificationresponse;

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
        name = "dhinternal.io.deephaven_core.proto.session_pb.TerminationNotificationResponse.StackTrace",
        namespace = JsPackage.GLOBAL)
public class StackTrace {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static StackTrace.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<String> getElementsList();

        @JsProperty
        String getMessage();

        @JsProperty
        String getType();

        @JsProperty
        void setElementsList(JsArray<String> elementsList);

        @JsOverlay
        default void setElementsList(String[] elementsList) {
            setElementsList(Js.<JsArray<String>>uncheckedCast(elementsList));
        }

        @JsProperty
        void setMessage(String message);

        @JsProperty
        void setType(String type);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static StackTrace.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<String> getElementsList();

        @JsProperty
        String getMessage();

        @JsProperty
        String getType();

        @JsProperty
        void setElementsList(JsArray<String> elementsList);

        @JsOverlay
        default void setElementsList(String[] elementsList) {
            setElementsList(Js.<JsArray<String>>uncheckedCast(elementsList));
        }

        @JsProperty
        void setMessage(String message);

        @JsProperty
        void setType(String type);
    }

    public static native StackTrace deserializeBinary(Uint8Array bytes);

    public static native StackTrace deserializeBinaryFromReader(StackTrace message, Object reader);

    public static native void serializeBinaryToWriter(StackTrace message, Object writer);

    public static native StackTrace.ToObjectReturnType toObject(
            boolean includeInstance, StackTrace msg);

    public native String addElements(String value, double index);

    public native String addElements(String value);

    public native void clearElementsList();

    public native JsArray<String> getElementsList();

    public native String getMessage();

    public native String getType();

    public native Uint8Array serializeBinary();

    public native void setElementsList(JsArray<String> value);

    @JsOverlay
    public final void setElementsList(String[] value) {
        setElementsList(Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void setMessage(String value);

    public native void setType(String value);

    public native StackTrace.ToObjectReturnType0 toObject();

    public native StackTrace.ToObjectReturnType0 toObject(boolean includeInstance);
}
