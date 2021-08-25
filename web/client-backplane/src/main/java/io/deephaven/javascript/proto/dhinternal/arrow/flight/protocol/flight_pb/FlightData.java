package io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.arrow.flight.protocol.Flight_pb.FlightData",
    namespace = JsPackage.GLOBAL)
public class FlightData {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetAppMetadataUnionType {
        @JsOverlay
        static FlightData.GetAppMetadataUnionType of(Object o) {
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
    public interface GetDataBodyUnionType {
        @JsOverlay
        static FlightData.GetDataBodyUnionType of(Object o) {
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
    public interface GetDataHeaderUnionType {
        @JsOverlay
        static FlightData.GetDataHeaderUnionType of(Object o) {
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
        static FlightData.SetAppMetadataValueUnionType of(Object o) {
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
    public interface SetDataBodyValueUnionType {
        @JsOverlay
        static FlightData.SetDataBodyValueUnionType of(Object o) {
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
    public interface SetDataHeaderValueUnionType {
        @JsOverlay
        static FlightData.SetDataHeaderValueUnionType of(Object o) {
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
        public interface FlightDescriptorFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetCmdUnionType {
                @JsOverlay
                static FlightData.ToObjectReturnType.FlightDescriptorFieldType.GetCmdUnionType of(
                    Object o) {
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
            static FlightData.ToObjectReturnType.FlightDescriptorFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            FlightData.ToObjectReturnType.FlightDescriptorFieldType.GetCmdUnionType getCmd();

            @JsProperty
            JsArray<String> getPathList();

            @JsProperty
            double getType();

            @JsProperty
            void setCmd(
                FlightData.ToObjectReturnType.FlightDescriptorFieldType.GetCmdUnionType cmd);

            @JsOverlay
            default void setCmd(String cmd) {
                setCmd(
                    Js.<FlightData.ToObjectReturnType.FlightDescriptorFieldType.GetCmdUnionType>uncheckedCast(
                        cmd));
            }

            @JsOverlay
            default void setCmd(Uint8Array cmd) {
                setCmd(
                    Js.<FlightData.ToObjectReturnType.FlightDescriptorFieldType.GetCmdUnionType>uncheckedCast(
                        cmd));
            }

            @JsProperty
            void setPathList(JsArray<String> pathList);

            @JsOverlay
            default void setPathList(String[] pathList) {
                setPathList(Js.<JsArray<String>>uncheckedCast(pathList));
            }

            @JsProperty
            void setType(double type);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetAppMetadataUnionType {
            @JsOverlay
            static FlightData.ToObjectReturnType.GetAppMetadataUnionType of(Object o) {
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
        public interface GetDataBodyUnionType {
            @JsOverlay
            static FlightData.ToObjectReturnType.GetDataBodyUnionType of(Object o) {
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
        public interface GetDataHeaderUnionType {
            @JsOverlay
            static FlightData.ToObjectReturnType.GetDataHeaderUnionType of(Object o) {
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
        static FlightData.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        FlightData.ToObjectReturnType.GetAppMetadataUnionType getAppMetadata();

        @JsProperty
        FlightData.ToObjectReturnType.GetDataBodyUnionType getDataBody();

        @JsProperty
        FlightData.ToObjectReturnType.GetDataHeaderUnionType getDataHeader();

        @JsProperty
        FlightData.ToObjectReturnType.FlightDescriptorFieldType getFlightDescriptor();

        @JsProperty
        void setAppMetadata(FlightData.ToObjectReturnType.GetAppMetadataUnionType appMetadata);

        @JsOverlay
        default void setAppMetadata(String appMetadata) {
            setAppMetadata(
                Js.<FlightData.ToObjectReturnType.GetAppMetadataUnionType>uncheckedCast(
                    appMetadata));
        }

        @JsOverlay
        default void setAppMetadata(Uint8Array appMetadata) {
            setAppMetadata(
                Js.<FlightData.ToObjectReturnType.GetAppMetadataUnionType>uncheckedCast(
                    appMetadata));
        }

        @JsProperty
        void setDataBody(FlightData.ToObjectReturnType.GetDataBodyUnionType dataBody);

        @JsOverlay
        default void setDataBody(String dataBody) {
            setDataBody(
                Js.<FlightData.ToObjectReturnType.GetDataBodyUnionType>uncheckedCast(dataBody));
        }

        @JsOverlay
        default void setDataBody(Uint8Array dataBody) {
            setDataBody(
                Js.<FlightData.ToObjectReturnType.GetDataBodyUnionType>uncheckedCast(dataBody));
        }

        @JsProperty
        void setDataHeader(FlightData.ToObjectReturnType.GetDataHeaderUnionType dataHeader);

        @JsOverlay
        default void setDataHeader(String dataHeader) {
            setDataHeader(
                Js.<FlightData.ToObjectReturnType.GetDataHeaderUnionType>uncheckedCast(dataHeader));
        }

        @JsOverlay
        default void setDataHeader(Uint8Array dataHeader) {
            setDataHeader(
                Js.<FlightData.ToObjectReturnType.GetDataHeaderUnionType>uncheckedCast(dataHeader));
        }

        @JsProperty
        void setFlightDescriptor(
            FlightData.ToObjectReturnType.FlightDescriptorFieldType flightDescriptor);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface FlightDescriptorFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetCmdUnionType {
                @JsOverlay
                static FlightData.ToObjectReturnType0.FlightDescriptorFieldType.GetCmdUnionType of(
                    Object o) {
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
            static FlightData.ToObjectReturnType0.FlightDescriptorFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            FlightData.ToObjectReturnType0.FlightDescriptorFieldType.GetCmdUnionType getCmd();

            @JsProperty
            JsArray<String> getPathList();

            @JsProperty
            double getType();

            @JsProperty
            void setCmd(
                FlightData.ToObjectReturnType0.FlightDescriptorFieldType.GetCmdUnionType cmd);

            @JsOverlay
            default void setCmd(String cmd) {
                setCmd(
                    Js.<FlightData.ToObjectReturnType0.FlightDescriptorFieldType.GetCmdUnionType>uncheckedCast(
                        cmd));
            }

            @JsOverlay
            default void setCmd(Uint8Array cmd) {
                setCmd(
                    Js.<FlightData.ToObjectReturnType0.FlightDescriptorFieldType.GetCmdUnionType>uncheckedCast(
                        cmd));
            }

            @JsProperty
            void setPathList(JsArray<String> pathList);

            @JsOverlay
            default void setPathList(String[] pathList) {
                setPathList(Js.<JsArray<String>>uncheckedCast(pathList));
            }

            @JsProperty
            void setType(double type);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetAppMetadataUnionType {
            @JsOverlay
            static FlightData.ToObjectReturnType0.GetAppMetadataUnionType of(Object o) {
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
        public interface GetDataBodyUnionType {
            @JsOverlay
            static FlightData.ToObjectReturnType0.GetDataBodyUnionType of(Object o) {
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
        public interface GetDataHeaderUnionType {
            @JsOverlay
            static FlightData.ToObjectReturnType0.GetDataHeaderUnionType of(Object o) {
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
        static FlightData.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        FlightData.ToObjectReturnType0.GetAppMetadataUnionType getAppMetadata();

        @JsProperty
        FlightData.ToObjectReturnType0.GetDataBodyUnionType getDataBody();

        @JsProperty
        FlightData.ToObjectReturnType0.GetDataHeaderUnionType getDataHeader();

        @JsProperty
        FlightData.ToObjectReturnType0.FlightDescriptorFieldType getFlightDescriptor();

        @JsProperty
        void setAppMetadata(FlightData.ToObjectReturnType0.GetAppMetadataUnionType appMetadata);

        @JsOverlay
        default void setAppMetadata(String appMetadata) {
            setAppMetadata(
                Js.<FlightData.ToObjectReturnType0.GetAppMetadataUnionType>uncheckedCast(
                    appMetadata));
        }

        @JsOverlay
        default void setAppMetadata(Uint8Array appMetadata) {
            setAppMetadata(
                Js.<FlightData.ToObjectReturnType0.GetAppMetadataUnionType>uncheckedCast(
                    appMetadata));
        }

        @JsProperty
        void setDataBody(FlightData.ToObjectReturnType0.GetDataBodyUnionType dataBody);

        @JsOverlay
        default void setDataBody(String dataBody) {
            setDataBody(
                Js.<FlightData.ToObjectReturnType0.GetDataBodyUnionType>uncheckedCast(dataBody));
        }

        @JsOverlay
        default void setDataBody(Uint8Array dataBody) {
            setDataBody(
                Js.<FlightData.ToObjectReturnType0.GetDataBodyUnionType>uncheckedCast(dataBody));
        }

        @JsProperty
        void setDataHeader(FlightData.ToObjectReturnType0.GetDataHeaderUnionType dataHeader);

        @JsOverlay
        default void setDataHeader(String dataHeader) {
            setDataHeader(
                Js.<FlightData.ToObjectReturnType0.GetDataHeaderUnionType>uncheckedCast(
                    dataHeader));
        }

        @JsOverlay
        default void setDataHeader(Uint8Array dataHeader) {
            setDataHeader(
                Js.<FlightData.ToObjectReturnType0.GetDataHeaderUnionType>uncheckedCast(
                    dataHeader));
        }

        @JsProperty
        void setFlightDescriptor(
            FlightData.ToObjectReturnType0.FlightDescriptorFieldType flightDescriptor);
    }

    public static native FlightData deserializeBinary(Uint8Array bytes);

    public static native FlightData deserializeBinaryFromReader(FlightData message, Object reader);

    public static native void serializeBinaryToWriter(FlightData message, Object writer);

    public static native FlightData.ToObjectReturnType toObject(
        boolean includeInstance, FlightData msg);

    public native void clearFlightDescriptor();

    public native FlightData.GetAppMetadataUnionType getAppMetadata();

    public native String getAppMetadata_asB64();

    public native Uint8Array getAppMetadata_asU8();

    public native FlightData.GetDataBodyUnionType getDataBody();

    public native String getDataBody_asB64();

    public native Uint8Array getDataBody_asU8();

    public native FlightData.GetDataHeaderUnionType getDataHeader();

    public native String getDataHeader_asB64();

    public native Uint8Array getDataHeader_asU8();

    public native FlightDescriptor getFlightDescriptor();

    public native boolean hasFlightDescriptor();

    public native Uint8Array serializeBinary();

    public native void setAppMetadata(FlightData.SetAppMetadataValueUnionType value);

    @JsOverlay
    public final void setAppMetadata(String value) {
        setAppMetadata(Js.<FlightData.SetAppMetadataValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public final void setAppMetadata(Uint8Array value) {
        setAppMetadata(Js.<FlightData.SetAppMetadataValueUnionType>uncheckedCast(value));
    }

    public native void setDataBody(FlightData.SetDataBodyValueUnionType value);

    @JsOverlay
    public final void setDataBody(String value) {
        setDataBody(Js.<FlightData.SetDataBodyValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public final void setDataBody(Uint8Array value) {
        setDataBody(Js.<FlightData.SetDataBodyValueUnionType>uncheckedCast(value));
    }

    public native void setDataHeader(FlightData.SetDataHeaderValueUnionType value);

    @JsOverlay
    public final void setDataHeader(String value) {
        setDataHeader(Js.<FlightData.SetDataHeaderValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public final void setDataHeader(Uint8Array value) {
        setDataHeader(Js.<FlightData.SetDataHeaderValueUnionType>uncheckedCast(value));
    }

    public native void setFlightDescriptor();

    public native void setFlightDescriptor(FlightDescriptor value);

    public native FlightData.ToObjectReturnType0 toObject();

    public native FlightData.ToObjectReturnType0 toObject(boolean includeInstance);
}
