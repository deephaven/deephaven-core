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
        name = "dhinternal.io.deephaven.proto.console_pb.LogSubscriptionData",
        namespace = JsPackage.GLOBAL)
public class LogSubscriptionData {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static LogSubscriptionData.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getLogLevel();

        @JsProperty
        String getMessage();

        @JsProperty
        double getMicros();

        @JsProperty
        void setLogLevel(String logLevel);

        @JsProperty
        void setMessage(String message);

        @JsProperty
        void setMicros(double micros);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static LogSubscriptionData.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getLogLevel();

        @JsProperty
        String getMessage();

        @JsProperty
        double getMicros();

        @JsProperty
        void setLogLevel(String logLevel);

        @JsProperty
        void setMessage(String message);

        @JsProperty
        void setMicros(double micros);
    }

    public static native LogSubscriptionData deserializeBinary(Uint8Array bytes);

    public static native LogSubscriptionData deserializeBinaryFromReader(
            LogSubscriptionData message, Object reader);

    public static native void serializeBinaryToWriter(LogSubscriptionData message, Object writer);

    public static native LogSubscriptionData.ToObjectReturnType toObject(
            boolean includeInstance, LogSubscriptionData msg);

    public native String getLogLevel();

    public native String getMessage();

    public native double getMicros();

    public native Uint8Array serializeBinary();

    public native void setLogLevel(String value);

    public native void setMessage(String value);

    public native void setMicros(double value);

    public native LogSubscriptionData.ToObjectReturnType0 toObject();

    public native LogSubscriptionData.ToObjectReturnType0 toObject(boolean includeInstance);
}
