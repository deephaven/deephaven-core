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
        name = "dhinternal.io.deephaven_core.proto.storage_pb.SaveFileRequest",
        namespace = JsPackage.GLOBAL)
public class SaveFileRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetContentsUnionType {
        @JsOverlay
        static SaveFileRequest.GetContentsUnionType of(Object o) {
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
        static SaveFileRequest.SetContentsValueUnionType of(Object o) {
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
            static SaveFileRequest.ToObjectReturnType.GetContentsUnionType of(Object o) {
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
        static SaveFileRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        SaveFileRequest.ToObjectReturnType.GetContentsUnionType getContents();

        @JsProperty
        String getPath();

        @JsProperty
        boolean isAllowOverwrite();

        @JsProperty
        void setAllowOverwrite(boolean allowOverwrite);

        @JsProperty
        void setContents(SaveFileRequest.ToObjectReturnType.GetContentsUnionType contents);

        @JsOverlay
        default void setContents(String contents) {
            setContents(
                    Js.<SaveFileRequest.ToObjectReturnType.GetContentsUnionType>uncheckedCast(contents));
        }

        @JsOverlay
        default void setContents(Uint8Array contents) {
            setContents(
                    Js.<SaveFileRequest.ToObjectReturnType.GetContentsUnionType>uncheckedCast(contents));
        }

        @JsProperty
        void setPath(String path);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetContentsUnionType {
            @JsOverlay
            static SaveFileRequest.ToObjectReturnType0.GetContentsUnionType of(Object o) {
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
        static SaveFileRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        SaveFileRequest.ToObjectReturnType0.GetContentsUnionType getContents();

        @JsProperty
        String getPath();

        @JsProperty
        boolean isAllowOverwrite();

        @JsProperty
        void setAllowOverwrite(boolean allowOverwrite);

        @JsProperty
        void setContents(SaveFileRequest.ToObjectReturnType0.GetContentsUnionType contents);

        @JsOverlay
        default void setContents(String contents) {
            setContents(
                    Js.<SaveFileRequest.ToObjectReturnType0.GetContentsUnionType>uncheckedCast(contents));
        }

        @JsOverlay
        default void setContents(Uint8Array contents) {
            setContents(
                    Js.<SaveFileRequest.ToObjectReturnType0.GetContentsUnionType>uncheckedCast(contents));
        }

        @JsProperty
        void setPath(String path);
    }

    public static native SaveFileRequest deserializeBinary(Uint8Array bytes);

    public static native SaveFileRequest deserializeBinaryFromReader(
            SaveFileRequest message, Object reader);

    public static native void serializeBinaryToWriter(SaveFileRequest message, Object writer);

    public static native SaveFileRequest.ToObjectReturnType toObject(
            boolean includeInstance, SaveFileRequest msg);

    public native boolean getAllowOverwrite();

    public native SaveFileRequest.GetContentsUnionType getContents();

    public native String getContents_asB64();

    public native Uint8Array getContents_asU8();

    public native String getPath();

    public native Uint8Array serializeBinary();

    public native void setAllowOverwrite(boolean value);

    public native void setContents(SaveFileRequest.SetContentsValueUnionType value);

    @JsOverlay
    public final void setContents(String value) {
        setContents(Js.<SaveFileRequest.SetContentsValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public final void setContents(Uint8Array value) {
        setContents(Js.<SaveFileRequest.SetContentsValueUnionType>uncheckedCast(value));
    }

    public native void setPath(String value);

    public native SaveFileRequest.ToObjectReturnType0 toObject();

    public native SaveFileRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
