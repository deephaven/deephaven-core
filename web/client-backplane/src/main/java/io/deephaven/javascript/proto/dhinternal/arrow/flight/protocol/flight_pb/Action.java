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
    name = "dhinternal.arrow.flight.protocol.Flight_pb.Action",
    namespace = JsPackage.GLOBAL)
public class Action {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetBodyUnionType {
        @JsOverlay
        static Action.GetBodyUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default String asString() {
            return Js.asString(this);
        }

        @JsOverlay
        default Uint8Array asUint8Array() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isString() {
            return (Object) this instanceof String;
        }

        @JsOverlay
        default boolean isUint8Array() {
            return (Object) this instanceof Uint8Array;
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SetBodyValueUnionType {
        @JsOverlay
        static Action.SetBodyValueUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default String asString() {
            return Js.asString(this);
        }

        @JsOverlay
        default Uint8Array asUint8Array() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isString() {
            return (Object) this instanceof String;
        }

        @JsOverlay
        default boolean isUint8Array() {
            return (Object) this instanceof Uint8Array;
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetBodyUnionType {
            @JsOverlay
            static Action.ToObjectReturnType.GetBodyUnionType of(Object o) {
                return Js.cast(o);
            }

            @JsOverlay
            default String asString() {
                return Js.asString(this);
            }

            @JsOverlay
            default Uint8Array asUint8Array() {
                return Js.cast(this);
            }

            @JsOverlay
            default boolean isString() {
                return (Object) this instanceof String;
            }

            @JsOverlay
            default boolean isUint8Array() {
                return (Object) this instanceof Uint8Array;
            }
        }

        @JsOverlay
        static Action.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Action.ToObjectReturnType.GetBodyUnionType getBody();

        @JsProperty
        String getType();

        @JsProperty
        void setBody(Action.ToObjectReturnType.GetBodyUnionType body);

        @JsOverlay
        default void setBody(String body) {
            setBody(Js.<Action.ToObjectReturnType.GetBodyUnionType>uncheckedCast(body));
        }

        @JsOverlay
        default void setBody(Uint8Array body) {
            setBody(Js.<Action.ToObjectReturnType.GetBodyUnionType>uncheckedCast(body));
        }

        @JsProperty
        void setType(String type);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetBodyUnionType {
            @JsOverlay
            static Action.ToObjectReturnType0.GetBodyUnionType of(Object o) {
                return Js.cast(o);
            }

            @JsOverlay
            default String asString() {
                return Js.asString(this);
            }

            @JsOverlay
            default Uint8Array asUint8Array() {
                return Js.cast(this);
            }

            @JsOverlay
            default boolean isString() {
                return (Object) this instanceof String;
            }

            @JsOverlay
            default boolean isUint8Array() {
                return (Object) this instanceof Uint8Array;
            }
        }

        @JsOverlay
        static Action.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Action.ToObjectReturnType0.GetBodyUnionType getBody();

        @JsProperty
        String getType();

        @JsProperty
        void setBody(Action.ToObjectReturnType0.GetBodyUnionType body);

        @JsOverlay
        default void setBody(String body) {
            setBody(Js.<Action.ToObjectReturnType0.GetBodyUnionType>uncheckedCast(body));
        }

        @JsOverlay
        default void setBody(Uint8Array body) {
            setBody(Js.<Action.ToObjectReturnType0.GetBodyUnionType>uncheckedCast(body));
        }

        @JsProperty
        void setType(String type);
    }

    public static native Action deserializeBinary(Uint8Array bytes);

    public static native Action deserializeBinaryFromReader(Action message, Object reader);

    public static native void serializeBinaryToWriter(Action message, Object writer);

    public static native Action.ToObjectReturnType toObject(boolean includeInstance, Action msg);

    public native Action.GetBodyUnionType getBody();

    public native String getBody_asB64();

    public native Uint8Array getBody_asU8();

    public native String getType();

    public native Uint8Array serializeBinary();

    public native void setBody(Action.SetBodyValueUnionType value);

    @JsOverlay
    public final void setBody(String value) {
        setBody(Js.<Action.SetBodyValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public final void setBody(Uint8Array value) {
        setBody(Js.<Action.SetBodyValueUnionType>uncheckedCast(value));
    }

    public native void setType(String value);

    public native Action.ToObjectReturnType0 toObject();

    public native Action.ToObjectReturnType0 toObject(boolean includeInstance);
}
