package io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.arrow.flight.protocol.Flight_pb.BasicAuth",
        namespace = JsPackage.GLOBAL)
public class BasicAuth {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static BasicAuth.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getPassword();

        @JsProperty
        String getUsername();

        @JsProperty
        void setPassword(String password);

        @JsProperty
        void setUsername(String username);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static BasicAuth.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getPassword();

        @JsProperty
        String getUsername();

        @JsProperty
        void setPassword(String password);

        @JsProperty
        void setUsername(String username);
    }

    public static native BasicAuth deserializeBinary(Uint8Array bytes);

    public static native BasicAuth deserializeBinaryFromReader(BasicAuth message, Object reader);

    public static native void serializeBinaryToWriter(BasicAuth message, Object writer);

    public static native BasicAuth.ToObjectReturnType toObject(
            boolean includeInstance, BasicAuth msg);

    public native String getPassword();

    public native String getUsername();

    public native Uint8Array serializeBinary();

    public native void setPassword(String value);

    public native void setUsername(String value);

    public native BasicAuth.ToObjectReturnType0 toObject();

    public native BasicAuth.ToObjectReturnType0 toObject(boolean includeInstance);
}
