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
        name = "dhinternal.arrow.flight.protocol.Flight_pb.Location",
        namespace = JsPackage.GLOBAL)
public class Location {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static Location.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getUri();

        @JsProperty
        void setUri(String uri);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static Location.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getUri();

        @JsProperty
        void setUri(String uri);
    }

    public static native Location deserializeBinary(Uint8Array bytes);

    public static native Location deserializeBinaryFromReader(Location message, Object reader);

    public static native void serializeBinaryToWriter(Location message, Object writer);

    public static native Location.ToObjectReturnType toObject(boolean includeInstance, Location msg);

    public native String getUri();

    public native Uint8Array serializeBinary();

    public native void setUri(String value);

    public native Location.ToObjectReturnType0 toObject();

    public native Location.ToObjectReturnType0 toObject(boolean includeInstance);
}
