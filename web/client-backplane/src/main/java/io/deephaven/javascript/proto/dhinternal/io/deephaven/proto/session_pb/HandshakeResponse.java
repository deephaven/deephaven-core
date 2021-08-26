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
        name = "dhinternal.io.deephaven.proto.session_pb.HandshakeResponse",
        namespace = JsPackage.GLOBAL)
public class HandshakeResponse {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetMetadataHeaderUnionType {
        @JsOverlay
        static HandshakeResponse.GetMetadataHeaderUnionType of(Object o) {
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
    public interface GetSessionTokenUnionType {
        @JsOverlay
        static HandshakeResponse.GetSessionTokenUnionType of(Object o) {
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
    public interface SetMetadataHeaderValueUnionType {
        @JsOverlay
        static HandshakeResponse.SetMetadataHeaderValueUnionType of(Object o) {
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
    public interface SetSessionTokenValueUnionType {
        @JsOverlay
        static HandshakeResponse.SetSessionTokenValueUnionType of(Object o) {
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
        public interface GetMetadataHeaderUnionType {
            @JsOverlay
            static HandshakeResponse.ToObjectReturnType.GetMetadataHeaderUnionType of(Object o) {
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
        public interface GetSessionTokenUnionType {
            @JsOverlay
            static HandshakeResponse.ToObjectReturnType.GetSessionTokenUnionType of(Object o) {
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
        static HandshakeResponse.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        HandshakeResponse.ToObjectReturnType.GetMetadataHeaderUnionType getMetadataHeader();

        @JsProperty
        HandshakeResponse.ToObjectReturnType.GetSessionTokenUnionType getSessionToken();

        @JsProperty
        String getTokenDeadlineTimeMillis();

        @JsProperty
        String getTokenExpirationDelayMillis();

        @JsProperty
        void setMetadataHeader(
                HandshakeResponse.ToObjectReturnType.GetMetadataHeaderUnionType metadataHeader);

        @JsOverlay
        default void setMetadataHeader(String metadataHeader) {
            setMetadataHeader(
                    Js.<HandshakeResponse.ToObjectReturnType.GetMetadataHeaderUnionType>uncheckedCast(
                            metadataHeader));
        }

        @JsOverlay
        default void setMetadataHeader(Uint8Array metadataHeader) {
            setMetadataHeader(
                    Js.<HandshakeResponse.ToObjectReturnType.GetMetadataHeaderUnionType>uncheckedCast(
                            metadataHeader));
        }

        @JsProperty
        void setSessionToken(
                HandshakeResponse.ToObjectReturnType.GetSessionTokenUnionType sessionToken);

        @JsOverlay
        default void setSessionToken(String sessionToken) {
            setSessionToken(
                    Js.<HandshakeResponse.ToObjectReturnType.GetSessionTokenUnionType>uncheckedCast(
                            sessionToken));
        }

        @JsOverlay
        default void setSessionToken(Uint8Array sessionToken) {
            setSessionToken(
                    Js.<HandshakeResponse.ToObjectReturnType.GetSessionTokenUnionType>uncheckedCast(
                            sessionToken));
        }

        @JsProperty
        void setTokenDeadlineTimeMillis(String tokenDeadlineTimeMillis);

        @JsProperty
        void setTokenExpirationDelayMillis(String tokenExpirationDelayMillis);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetMetadataHeaderUnionType {
            @JsOverlay
            static HandshakeResponse.ToObjectReturnType0.GetMetadataHeaderUnionType of(Object o) {
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
        public interface GetSessionTokenUnionType {
            @JsOverlay
            static HandshakeResponse.ToObjectReturnType0.GetSessionTokenUnionType of(Object o) {
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
        static HandshakeResponse.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        HandshakeResponse.ToObjectReturnType0.GetMetadataHeaderUnionType getMetadataHeader();

        @JsProperty
        HandshakeResponse.ToObjectReturnType0.GetSessionTokenUnionType getSessionToken();

        @JsProperty
        String getTokenDeadlineTimeMillis();

        @JsProperty
        String getTokenExpirationDelayMillis();

        @JsProperty
        void setMetadataHeader(
                HandshakeResponse.ToObjectReturnType0.GetMetadataHeaderUnionType metadataHeader);

        @JsOverlay
        default void setMetadataHeader(String metadataHeader) {
            setMetadataHeader(
                    Js.<HandshakeResponse.ToObjectReturnType0.GetMetadataHeaderUnionType>uncheckedCast(
                            metadataHeader));
        }

        @JsOverlay
        default void setMetadataHeader(Uint8Array metadataHeader) {
            setMetadataHeader(
                    Js.<HandshakeResponse.ToObjectReturnType0.GetMetadataHeaderUnionType>uncheckedCast(
                            metadataHeader));
        }

        @JsProperty
        void setSessionToken(
                HandshakeResponse.ToObjectReturnType0.GetSessionTokenUnionType sessionToken);

        @JsOverlay
        default void setSessionToken(String sessionToken) {
            setSessionToken(
                    Js.<HandshakeResponse.ToObjectReturnType0.GetSessionTokenUnionType>uncheckedCast(
                            sessionToken));
        }

        @JsOverlay
        default void setSessionToken(Uint8Array sessionToken) {
            setSessionToken(
                    Js.<HandshakeResponse.ToObjectReturnType0.GetSessionTokenUnionType>uncheckedCast(
                            sessionToken));
        }

        @JsProperty
        void setTokenDeadlineTimeMillis(String tokenDeadlineTimeMillis);

        @JsProperty
        void setTokenExpirationDelayMillis(String tokenExpirationDelayMillis);
    }

    public static native HandshakeResponse deserializeBinary(Uint8Array bytes);

    public static native HandshakeResponse deserializeBinaryFromReader(
            HandshakeResponse message, Object reader);

    public static native void serializeBinaryToWriter(HandshakeResponse message, Object writer);

    public static native HandshakeResponse.ToObjectReturnType toObject(
            boolean includeInstance, HandshakeResponse msg);

    public native HandshakeResponse.GetMetadataHeaderUnionType getMetadataHeader();

    public native String getMetadataHeader_asB64();

    public native Uint8Array getMetadataHeader_asU8();

    public native HandshakeResponse.GetSessionTokenUnionType getSessionToken();

    public native String getSessionToken_asB64();

    public native Uint8Array getSessionToken_asU8();

    public native String getTokenDeadlineTimeMillis();

    public native String getTokenExpirationDelayMillis();

    public native Uint8Array serializeBinary();

    public native void setMetadataHeader(HandshakeResponse.SetMetadataHeaderValueUnionType value);

    @JsOverlay
    public final void setMetadataHeader(String value) {
        setMetadataHeader(Js.<HandshakeResponse.SetMetadataHeaderValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public final void setMetadataHeader(Uint8Array value) {
        setMetadataHeader(Js.<HandshakeResponse.SetMetadataHeaderValueUnionType>uncheckedCast(value));
    }

    public native void setSessionToken(HandshakeResponse.SetSessionTokenValueUnionType value);

    @JsOverlay
    public final void setSessionToken(String value) {
        setSessionToken(Js.<HandshakeResponse.SetSessionTokenValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public final void setSessionToken(Uint8Array value) {
        setSessionToken(Js.<HandshakeResponse.SetSessionTokenValueUnionType>uncheckedCast(value));
    }

    public native void setTokenDeadlineTimeMillis(String value);

    public native void setTokenExpirationDelayMillis(String value);

    public native HandshakeResponse.ToObjectReturnType0 toObject();

    public native HandshakeResponse.ToObjectReturnType0 toObject(boolean includeInstance);
}
