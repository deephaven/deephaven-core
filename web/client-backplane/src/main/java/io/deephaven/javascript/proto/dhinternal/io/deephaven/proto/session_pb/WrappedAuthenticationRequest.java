package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.session_pb.WrappedAuthenticationRequest",
        namespace = JsPackage.GLOBAL)
public class WrappedAuthenticationRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetPayloadUnionType {
        @JsOverlay
        static WrappedAuthenticationRequest.GetPayloadUnionType of(Object o) {
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
    public interface SetPayloadValueUnionType {
        @JsOverlay
        static WrappedAuthenticationRequest.SetPayloadValueUnionType of(Object o) {
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
        public interface GetPayloadUnionType {
            @JsOverlay
            static WrappedAuthenticationRequest.ToObjectReturnType.GetPayloadUnionType of(Object o) {
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
        static WrappedAuthenticationRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        WrappedAuthenticationRequest.ToObjectReturnType.GetPayloadUnionType getPayload();

        @JsProperty
        String getType();

        @JsProperty
        void setPayload(WrappedAuthenticationRequest.ToObjectReturnType.GetPayloadUnionType payload);

        @JsOverlay
        default void setPayload(String payload) {
            setPayload(
                    Js.<WrappedAuthenticationRequest.ToObjectReturnType.GetPayloadUnionType>uncheckedCast(
                            payload));
        }

        @JsOverlay
        default void setPayload(Uint8Array payload) {
            setPayload(
                    Js.<WrappedAuthenticationRequest.ToObjectReturnType.GetPayloadUnionType>uncheckedCast(
                            payload));
        }

        @JsProperty
        void setType(String type);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetPayloadUnionType {
            @JsOverlay
            static WrappedAuthenticationRequest.ToObjectReturnType0.GetPayloadUnionType of(Object o) {
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
        static WrappedAuthenticationRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        WrappedAuthenticationRequest.ToObjectReturnType0.GetPayloadUnionType getPayload();

        @JsProperty
        String getType();

        @JsProperty
        void setPayload(WrappedAuthenticationRequest.ToObjectReturnType0.GetPayloadUnionType payload);

        @JsOverlay
        default void setPayload(String payload) {
            setPayload(
                    Js.<WrappedAuthenticationRequest.ToObjectReturnType0.GetPayloadUnionType>uncheckedCast(
                            payload));
        }

        @JsOverlay
        default void setPayload(Uint8Array payload) {
            setPayload(
                    Js.<WrappedAuthenticationRequest.ToObjectReturnType0.GetPayloadUnionType>uncheckedCast(
                            payload));
        }

        @JsProperty
        void setType(String type);
    }

    public static native WrappedAuthenticationRequest deserializeBinary(Uint8Array bytes);

    public static native WrappedAuthenticationRequest deserializeBinaryFromReader(
            WrappedAuthenticationRequest message, Object reader);

    public static native void serializeBinaryToWriter(
            WrappedAuthenticationRequest message, Object writer);

    public static native WrappedAuthenticationRequest.ToObjectReturnType toObject(
            boolean includeInstance, WrappedAuthenticationRequest msg);

    public native WrappedAuthenticationRequest.GetPayloadUnionType getPayload();

    public native String getPayload_asB64();

    public native Uint8Array getPayload_asU8();

    public native String getType();

    public native Uint8Array serializeBinary();

    public native void setPayload(WrappedAuthenticationRequest.SetPayloadValueUnionType value);

    @JsOverlay
    public final void setPayload(String value) {
        setPayload(Js.<WrappedAuthenticationRequest.SetPayloadValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public final void setPayload(Uint8Array value) {
        setPayload(Js.<WrappedAuthenticationRequest.SetPayloadValueUnionType>uncheckedCast(value));
    }

    public native void setType(String value);

    public native WrappedAuthenticationRequest.ToObjectReturnType0 toObject();

    public native WrappedAuthenticationRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
