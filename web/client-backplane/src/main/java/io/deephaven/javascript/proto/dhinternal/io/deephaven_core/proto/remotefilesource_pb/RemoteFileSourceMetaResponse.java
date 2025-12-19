//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.remotefilesource_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.remotefilesource_pb.RemoteFileSourceMetaResponse",
        namespace = JsPackage.GLOBAL)
public class RemoteFileSourceMetaResponse {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetContentUnionType {
        @JsOverlay
        static RemoteFileSourceMetaResponse.GetContentUnionType of(Object o) {
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
    public interface SetContentValueUnionType {
        @JsOverlay
        static RemoteFileSourceMetaResponse.SetContentValueUnionType of(Object o) {
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
        public interface GetContentUnionType {
            @JsOverlay
            static RemoteFileSourceMetaResponse.ToObjectReturnType.GetContentUnionType of(Object o) {
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
        static RemoteFileSourceMetaResponse.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        RemoteFileSourceMetaResponse.ToObjectReturnType.GetContentUnionType getContent();

        @JsProperty
        String getError();

        @JsProperty
        boolean isFound();

        @JsProperty
        void setContent(RemoteFileSourceMetaResponse.ToObjectReturnType.GetContentUnionType content);

        @JsOverlay
        default void setContent(String content) {
            setContent(
                    Js.<RemoteFileSourceMetaResponse.ToObjectReturnType.GetContentUnionType>uncheckedCast(
                            content));
        }

        @JsOverlay
        default void setContent(Uint8Array content) {
            setContent(
                    Js.<RemoteFileSourceMetaResponse.ToObjectReturnType.GetContentUnionType>uncheckedCast(
                            content));
        }

        @JsProperty
        void setError(String error);

        @JsProperty
        void setFound(boolean found);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetContentUnionType {
            @JsOverlay
            static RemoteFileSourceMetaResponse.ToObjectReturnType0.GetContentUnionType of(Object o) {
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
        static RemoteFileSourceMetaResponse.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        RemoteFileSourceMetaResponse.ToObjectReturnType0.GetContentUnionType getContent();

        @JsProperty
        String getError();

        @JsProperty
        boolean isFound();

        @JsProperty
        void setContent(RemoteFileSourceMetaResponse.ToObjectReturnType0.GetContentUnionType content);

        @JsOverlay
        default void setContent(String content) {
            setContent(
                    Js.<RemoteFileSourceMetaResponse.ToObjectReturnType0.GetContentUnionType>uncheckedCast(
                            content));
        }

        @JsOverlay
        default void setContent(Uint8Array content) {
            setContent(
                    Js.<RemoteFileSourceMetaResponse.ToObjectReturnType0.GetContentUnionType>uncheckedCast(
                            content));
        }

        @JsProperty
        void setError(String error);

        @JsProperty
        void setFound(boolean found);
    }

    public static native RemoteFileSourceMetaResponse deserializeBinary(Uint8Array bytes);

    public static native RemoteFileSourceMetaResponse deserializeBinaryFromReader(
            RemoteFileSourceMetaResponse message, Object reader);

    public static native void serializeBinaryToWriter(
            RemoteFileSourceMetaResponse message, Object writer);

    public static native RemoteFileSourceMetaResponse.ToObjectReturnType toObject(
            boolean includeInstance, RemoteFileSourceMetaResponse msg);

    public native RemoteFileSourceMetaResponse.GetContentUnionType getContent();

    public native String getContent_asB64();

    public native Uint8Array getContent_asU8();

    public native String getError();

    public native boolean getFound();

    public native Uint8Array serializeBinary();

    public native void setContent(RemoteFileSourceMetaResponse.SetContentValueUnionType value);

    @JsOverlay
    public final void setContent(String value) {
        setContent(Js.<RemoteFileSourceMetaResponse.SetContentValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public final void setContent(Uint8Array value) {
        setContent(Js.<RemoteFileSourceMetaResponse.SetContentValueUnionType>uncheckedCast(value));
    }

    public native void setError(String value);

    public native void setFound(boolean value);

    public native RemoteFileSourceMetaResponse.ToObjectReturnType0 toObject();

    public native RemoteFileSourceMetaResponse.ToObjectReturnType0 toObject(boolean includeInstance);
}
