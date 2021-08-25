package io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.browserflight_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.arrow.flight.protocol.BrowserFlight_pb.BrowserNextResponse",
        namespace = JsPackage.GLOBAL)
public class BrowserNextResponse {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetAppMetadataUnionType {
        @JsOverlay
        static BrowserNextResponse.GetAppMetadataUnionType of(Object o) {
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
    public interface SetAppMetadataValueUnionType {
        @JsOverlay
        static BrowserNextResponse.SetAppMetadataValueUnionType of(Object o) {
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
        public interface GetAppMetadataUnionType {
            @JsOverlay
            static BrowserNextResponse.ToObjectReturnType.GetAppMetadataUnionType of(Object o) {
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
        static BrowserNextResponse.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        BrowserNextResponse.ToObjectReturnType.GetAppMetadataUnionType getAppMetadata();

        @JsProperty
        void setAppMetadata(BrowserNextResponse.ToObjectReturnType.GetAppMetadataUnionType appMetadata);

        @JsOverlay
        default void setAppMetadata(String appMetadata) {
            setAppMetadata(
                    Js.<BrowserNextResponse.ToObjectReturnType.GetAppMetadataUnionType>uncheckedCast(
                            appMetadata));
        }

        @JsOverlay
        default void setAppMetadata(Uint8Array appMetadata) {
            setAppMetadata(
                    Js.<BrowserNextResponse.ToObjectReturnType.GetAppMetadataUnionType>uncheckedCast(
                            appMetadata));
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetAppMetadataUnionType {
            @JsOverlay
            static BrowserNextResponse.ToObjectReturnType0.GetAppMetadataUnionType of(Object o) {
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
        static BrowserNextResponse.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        BrowserNextResponse.ToObjectReturnType0.GetAppMetadataUnionType getAppMetadata();

        @JsProperty
        void setAppMetadata(
                BrowserNextResponse.ToObjectReturnType0.GetAppMetadataUnionType appMetadata);

        @JsOverlay
        default void setAppMetadata(String appMetadata) {
            setAppMetadata(
                    Js.<BrowserNextResponse.ToObjectReturnType0.GetAppMetadataUnionType>uncheckedCast(
                            appMetadata));
        }

        @JsOverlay
        default void setAppMetadata(Uint8Array appMetadata) {
            setAppMetadata(
                    Js.<BrowserNextResponse.ToObjectReturnType0.GetAppMetadataUnionType>uncheckedCast(
                            appMetadata));
        }
    }

    public static native BrowserNextResponse deserializeBinary(Uint8Array bytes);

    public static native BrowserNextResponse deserializeBinaryFromReader(
            BrowserNextResponse message, Object reader);

    public static native void serializeBinaryToWriter(BrowserNextResponse message, Object writer);

    public static native BrowserNextResponse.ToObjectReturnType toObject(
            boolean includeInstance, BrowserNextResponse msg);

    public native BrowserNextResponse.GetAppMetadataUnionType getAppMetadata();

    public native String getAppMetadata_asB64();

    public native Uint8Array getAppMetadata_asU8();

    public native Uint8Array serializeBinary();

    public native void setAppMetadata(BrowserNextResponse.SetAppMetadataValueUnionType value);

    @JsOverlay
    public final void setAppMetadata(String value) {
        setAppMetadata(Js.<BrowserNextResponse.SetAppMetadataValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public final void setAppMetadata(Uint8Array value) {
        setAppMetadata(Js.<BrowserNextResponse.SetAppMetadataValueUnionType>uncheckedCast(value));
    }

    public native BrowserNextResponse.ToObjectReturnType0 toObject();

    public native BrowserNextResponse.ToObjectReturnType0 toObject(boolean includeInstance);
}
