package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.application_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.application_pb.TableInfo",
        namespace = JsPackage.GLOBAL)
public class TableInfo {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetSchemaHeaderUnionType {
        @JsOverlay
        static TableInfo.GetSchemaHeaderUnionType of(Object o) {
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
    public interface SetSchemaHeaderValueUnionType {
        @JsOverlay
        static TableInfo.SetSchemaHeaderValueUnionType of(Object o) {
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
        public interface GetSchemaHeaderUnionType {
            @JsOverlay
            static TableInfo.ToObjectReturnType.GetSchemaHeaderUnionType of(Object o) {
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
        static TableInfo.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        TableInfo.ToObjectReturnType.GetSchemaHeaderUnionType getSchemaHeader();

        @JsProperty
        String getSize();

        @JsProperty
        boolean isIsStatic();

        @JsProperty
        void setIsStatic(boolean isStatic);

        @JsProperty
        void setSchemaHeader(TableInfo.ToObjectReturnType.GetSchemaHeaderUnionType schemaHeader);

        @JsOverlay
        default void setSchemaHeader(String schemaHeader) {
            setSchemaHeader(
                    Js.<TableInfo.ToObjectReturnType.GetSchemaHeaderUnionType>uncheckedCast(schemaHeader));
        }

        @JsOverlay
        default void setSchemaHeader(Uint8Array schemaHeader) {
            setSchemaHeader(
                    Js.<TableInfo.ToObjectReturnType.GetSchemaHeaderUnionType>uncheckedCast(schemaHeader));
        }

        @JsProperty
        void setSize(String size);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetSchemaHeaderUnionType {
            @JsOverlay
            static TableInfo.ToObjectReturnType0.GetSchemaHeaderUnionType of(Object o) {
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
        static TableInfo.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        TableInfo.ToObjectReturnType0.GetSchemaHeaderUnionType getSchemaHeader();

        @JsProperty
        String getSize();

        @JsProperty
        boolean isIsStatic();

        @JsProperty
        void setIsStatic(boolean isStatic);

        @JsProperty
        void setSchemaHeader(TableInfo.ToObjectReturnType0.GetSchemaHeaderUnionType schemaHeader);

        @JsOverlay
        default void setSchemaHeader(String schemaHeader) {
            setSchemaHeader(
                    Js.<TableInfo.ToObjectReturnType0.GetSchemaHeaderUnionType>uncheckedCast(schemaHeader));
        }

        @JsOverlay
        default void setSchemaHeader(Uint8Array schemaHeader) {
            setSchemaHeader(
                    Js.<TableInfo.ToObjectReturnType0.GetSchemaHeaderUnionType>uncheckedCast(schemaHeader));
        }

        @JsProperty
        void setSize(String size);
    }

    public static native TableInfo deserializeBinary(Uint8Array bytes);

    public static native TableInfo deserializeBinaryFromReader(TableInfo message, Object reader);

    public static native void serializeBinaryToWriter(TableInfo message, Object writer);

    public static native TableInfo.ToObjectReturnType toObject(
            boolean includeInstance, TableInfo msg);

    public native boolean getIsStatic();

    public native TableInfo.GetSchemaHeaderUnionType getSchemaHeader();

    public native String getSchemaHeader_asB64();

    public native Uint8Array getSchemaHeader_asU8();

    public native String getSize();

    public native Uint8Array serializeBinary();

    public native void setIsStatic(boolean value);

    public native void setSchemaHeader(TableInfo.SetSchemaHeaderValueUnionType value);

    @JsOverlay
    public final void setSchemaHeader(String value) {
        setSchemaHeader(Js.<TableInfo.SetSchemaHeaderValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public final void setSchemaHeader(Uint8Array value) {
        setSchemaHeader(Js.<TableInfo.SetSchemaHeaderValueUnionType>uncheckedCast(value));
    }

    public native void setSize(String value);

    public native TableInfo.ToObjectReturnType0 toObject();

    public native TableInfo.ToObjectReturnType0 toObject(boolean includeInstance);
}
