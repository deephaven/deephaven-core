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
    name = "dhinternal.io.deephaven.proto.console_pb.LogSubscriptionRequest",
    namespace = JsPackage.GLOBAL)
public class LogSubscriptionRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static LogSubscriptionRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getLastSeenLogTimestamp();

        @JsProperty
        JsArray<String> getLevelsList();

        @JsProperty
        void setLastSeenLogTimestamp(double lastSeenLogTimestamp);

        @JsProperty
        void setLevelsList(JsArray<String> levelsList);

        @JsOverlay
        default void setLevelsList(String[] levelsList) {
            setLevelsList(Js.<JsArray<String>>uncheckedCast(levelsList));
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static LogSubscriptionRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getLastSeenLogTimestamp();

        @JsProperty
        JsArray<String> getLevelsList();

        @JsProperty
        void setLastSeenLogTimestamp(double lastSeenLogTimestamp);

        @JsProperty
        void setLevelsList(JsArray<String> levelsList);

        @JsOverlay
        default void setLevelsList(String[] levelsList) {
            setLevelsList(Js.<JsArray<String>>uncheckedCast(levelsList));
        }
    }

    public static native LogSubscriptionRequest deserializeBinary(Uint8Array bytes);

    public static native LogSubscriptionRequest deserializeBinaryFromReader(
        LogSubscriptionRequest message, Object reader);

    public static native void serializeBinaryToWriter(LogSubscriptionRequest message,
        Object writer);

    public static native LogSubscriptionRequest.ToObjectReturnType toObject(
        boolean includeInstance, LogSubscriptionRequest msg);

    public native String addLevels(String value, double index);

    public native String addLevels(String value);

    public native void clearLevelsList();

    public native double getLastSeenLogTimestamp();

    public native JsArray<String> getLevelsList();

    public native Uint8Array serializeBinary();

    public native void setLastSeenLogTimestamp(double value);

    public native void setLevelsList(JsArray<String> value);

    @JsOverlay
    public final void setLevelsList(String[] value) {
        setLevelsList(Js.<JsArray<String>>uncheckedCast(value));
    }

    public native LogSubscriptionRequest.ToObjectReturnType0 toObject();

    public native LogSubscriptionRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
