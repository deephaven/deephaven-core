//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.storage_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.storage_pb.FetchFileResponse",
        namespace = JsPackage.GLOBAL)
public class FetchFileResponse {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetContentsUnionType {
        @JsOverlay
        static FetchFileResponse.GetContentsUnionType of(Object o) {
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
    public interface SetContentsValueUnionType {
        @JsOverlay
        static FetchFileResponse.SetContentsValueUnionType of(Object o) {
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
        public interface GetContentsUnionType {
            @JsOverlay
            static FetchFileResponse.ToObjectReturnType.GetContentsUnionType of(Object o) {
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
        static FetchFileResponse.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        FetchFileResponse.ToObjectReturnType.GetContentsUnionType getContents();

        @JsProperty
        String getEtag();

        @JsProperty
        void setContents(FetchFileResponse.ToObjectReturnType.GetContentsUnionType contents);

        @JsOverlay
        default void setContents(String contents) {
            setContents(
                    Js.<FetchFileResponse.ToObjectReturnType.GetContentsUnionType>uncheckedCast(contents));
        }

        @JsOverlay
        default void setContents(Uint8Array contents) {
            setContents(
                    Js.<FetchFileResponse.ToObjectReturnType.GetContentsUnionType>uncheckedCast(contents));
        }

        @JsProperty
        void setEtag(String etag);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetContentsUnionType {
            @JsOverlay
            static FetchFileResponse.ToObjectReturnType0.GetContentsUnionType of(Object o) {
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
        static FetchFileResponse.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        FetchFileResponse.ToObjectReturnType0.GetContentsUnionType getContents();

        @JsProperty
        String getEtag();

        @JsProperty
        void setContents(FetchFileResponse.ToObjectReturnType0.GetContentsUnionType contents);

        @JsOverlay
        default void setContents(String contents) {
            setContents(
                    Js.<FetchFileResponse.ToObjectReturnType0.GetContentsUnionType>uncheckedCast(contents));
        }

        @JsOverlay
        default void setContents(Uint8Array contents) {
            setContents(
                    Js.<FetchFileResponse.ToObjectReturnType0.GetContentsUnionType>uncheckedCast(contents));
        }

        @JsProperty
        void setEtag(String etag);
    }

    public static native FetchFileResponse deserializeBinary(Uint8Array bytes);

    public static native FetchFileResponse deserializeBinaryFromReader(
            FetchFileResponse message, Object reader);

    public static native void serializeBinaryToWriter(FetchFileResponse message, Object writer);

    public static native FetchFileResponse.ToObjectReturnType toObject(
            boolean includeInstance, FetchFileResponse msg);

    public native void clearEtag();

    public native FetchFileResponse.GetContentsUnionType getContents();

    public native String getContents_asB64();

    public native Uint8Array getContents_asU8();

    public native String getEtag();

    public native boolean hasEtag();

    public native Uint8Array serializeBinary();

    public native void setContents(FetchFileResponse.SetContentsValueUnionType value);

    @JsOverlay
    public final void setContents(String value) {
        setContents(Js.<FetchFileResponse.SetContentsValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public final void setContents(Uint8Array value) {
        setContents(Js.<FetchFileResponse.SetContentsValueUnionType>uncheckedCast(value));
    }

    public native void setEtag(String value);

    public native FetchFileResponse.ToObjectReturnType0 toObject();

    public native FetchFileResponse.ToObjectReturnType0 toObject(boolean includeInstance);
}
