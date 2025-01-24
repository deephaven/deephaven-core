//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.session_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.session_pb.terminationnotificationresponse.StackTrace;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.session_pb.TerminationNotificationResponse",
        namespace = JsPackage.GLOBAL)
public class TerminationNotificationResponse {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface StackTracesListFieldType {
            @JsOverlay
            static TerminationNotificationResponse.ToObjectReturnType.StackTracesListFieldType create() {
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

        @JsOverlay
        static TerminationNotificationResponse.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getReason();

        @JsProperty
        JsArray<TerminationNotificationResponse.ToObjectReturnType.StackTracesListFieldType> getStackTracesList();

        @JsProperty
        boolean isAbnormalTermination();

        @JsProperty
        boolean isIsFromUncaughtException();

        @JsProperty
        void setAbnormalTermination(boolean abnormalTermination);

        @JsProperty
        void setIsFromUncaughtException(boolean isFromUncaughtException);

        @JsProperty
        void setReason(String reason);

        @JsProperty
        void setStackTracesList(
                JsArray<TerminationNotificationResponse.ToObjectReturnType.StackTracesListFieldType> stackTracesList);

        @JsOverlay
        default void setStackTracesList(
                TerminationNotificationResponse.ToObjectReturnType.StackTracesListFieldType[] stackTracesList) {
            setStackTracesList(
                    Js.<JsArray<TerminationNotificationResponse.ToObjectReturnType.StackTracesListFieldType>>uncheckedCast(
                            stackTracesList));
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface StackTracesListFieldType {
            @JsOverlay
            static TerminationNotificationResponse.ToObjectReturnType0.StackTracesListFieldType create() {
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

        @JsOverlay
        static TerminationNotificationResponse.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getReason();

        @JsProperty
        JsArray<TerminationNotificationResponse.ToObjectReturnType0.StackTracesListFieldType> getStackTracesList();

        @JsProperty
        boolean isAbnormalTermination();

        @JsProperty
        boolean isIsFromUncaughtException();

        @JsProperty
        void setAbnormalTermination(boolean abnormalTermination);

        @JsProperty
        void setIsFromUncaughtException(boolean isFromUncaughtException);

        @JsProperty
        void setReason(String reason);

        @JsProperty
        void setStackTracesList(
                JsArray<TerminationNotificationResponse.ToObjectReturnType0.StackTracesListFieldType> stackTracesList);

        @JsOverlay
        default void setStackTracesList(
                TerminationNotificationResponse.ToObjectReturnType0.StackTracesListFieldType[] stackTracesList) {
            setStackTracesList(
                    Js.<JsArray<TerminationNotificationResponse.ToObjectReturnType0.StackTracesListFieldType>>uncheckedCast(
                            stackTracesList));
        }
    }

    public static native TerminationNotificationResponse deserializeBinary(Uint8Array bytes);

    public static native TerminationNotificationResponse deserializeBinaryFromReader(
            TerminationNotificationResponse message, Object reader);

    public static native void serializeBinaryToWriter(
            TerminationNotificationResponse message, Object writer);

    public static native TerminationNotificationResponse.ToObjectReturnType toObject(
            boolean includeInstance, TerminationNotificationResponse msg);

    public native StackTrace addStackTraces();

    public native StackTrace addStackTraces(StackTrace value, double index);

    public native StackTrace addStackTraces(StackTrace value);

    public native void clearStackTracesList();

    public native boolean getAbnormalTermination();

    public native boolean getIsFromUncaughtException();

    public native String getReason();

    public native JsArray<StackTrace> getStackTracesList();

    public native Uint8Array serializeBinary();

    public native void setAbnormalTermination(boolean value);

    public native void setIsFromUncaughtException(boolean value);

    public native void setReason(String value);

    public native void setStackTracesList(JsArray<StackTrace> value);

    @JsOverlay
    public final void setStackTracesList(StackTrace[] value) {
        setStackTracesList(Js.<JsArray<StackTrace>>uncheckedCast(value));
    }

    public native TerminationNotificationResponse.ToObjectReturnType0 toObject();

    public native TerminationNotificationResponse.ToObjectReturnType0 toObject(
            boolean includeInstance);
}
