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
        name = "dhinternal.arrow.flight.protocol.Flight_pb.FlightInfo",
        namespace = JsPackage.GLOBAL)
public class FlightInfo {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetSchemaUnionType {
        @JsOverlay
        static FlightInfo.GetSchemaUnionType of(Object o) {
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
    public interface SetSchemaValueUnionType {
        @JsOverlay
        static FlightInfo.SetSchemaValueUnionType of(Object o) {
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
        public interface EndpointListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface LocationListFieldType {
                @JsOverlay
                static FlightInfo.ToObjectReturnType.EndpointListFieldType.LocationListFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getUri();

                @JsProperty
                void setUri(String uri);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TicketFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface GetTicketUnionType {
                    @JsOverlay
                    static FlightInfo.ToObjectReturnType.EndpointListFieldType.TicketFieldType.GetTicketUnionType of(
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
                static FlightInfo.ToObjectReturnType.EndpointListFieldType.TicketFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                FlightInfo.ToObjectReturnType.EndpointListFieldType.TicketFieldType.GetTicketUnionType getTicket();

                @JsProperty
                void setTicket(
                        FlightInfo.ToObjectReturnType.EndpointListFieldType.TicketFieldType.GetTicketUnionType ticket);

                @JsOverlay
                default void setTicket(String ticket) {
                    setTicket(
                            Js.<FlightInfo.ToObjectReturnType.EndpointListFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }

                @JsOverlay
                default void setTicket(Uint8Array ticket) {
                    setTicket(
                            Js.<FlightInfo.ToObjectReturnType.EndpointListFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }
            }

            @JsOverlay
            static FlightInfo.ToObjectReturnType.EndpointListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<FlightInfo.ToObjectReturnType.EndpointListFieldType.LocationListFieldType> getLocationList();

            @JsProperty
            FlightInfo.ToObjectReturnType.EndpointListFieldType.TicketFieldType getTicket();

            @JsProperty
            void setLocationList(
                    JsArray<FlightInfo.ToObjectReturnType.EndpointListFieldType.LocationListFieldType> locationList);

            @JsOverlay
            default void setLocationList(
                    FlightInfo.ToObjectReturnType.EndpointListFieldType.LocationListFieldType[] locationList) {
                setLocationList(
                        Js.<JsArray<FlightInfo.ToObjectReturnType.EndpointListFieldType.LocationListFieldType>>uncheckedCast(
                                locationList));
            }

            @JsProperty
            void setTicket(FlightInfo.ToObjectReturnType.EndpointListFieldType.TicketFieldType ticket);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface FlightDescriptorFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetCmdUnionType {
                @JsOverlay
                static FlightInfo.ToObjectReturnType.FlightDescriptorFieldType.GetCmdUnionType of(
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
            static FlightInfo.ToObjectReturnType.FlightDescriptorFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            FlightInfo.ToObjectReturnType.FlightDescriptorFieldType.GetCmdUnionType getCmd();

            @JsProperty
            JsArray<String> getPathList();

            @JsProperty
            double getType();

            @JsProperty
            void setCmd(FlightInfo.ToObjectReturnType.FlightDescriptorFieldType.GetCmdUnionType cmd);

            @JsOverlay
            default void setCmd(String cmd) {
                setCmd(
                        Js.<FlightInfo.ToObjectReturnType.FlightDescriptorFieldType.GetCmdUnionType>uncheckedCast(cmd));
            }

            @JsOverlay
            default void setCmd(Uint8Array cmd) {
                setCmd(
                        Js.<FlightInfo.ToObjectReturnType.FlightDescriptorFieldType.GetCmdUnionType>uncheckedCast(cmd));
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
        public interface GetSchemaUnionType {
            @JsOverlay
            static FlightInfo.ToObjectReturnType.GetSchemaUnionType of(Object o) {
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
        static FlightInfo.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<FlightInfo.ToObjectReturnType.EndpointListFieldType> getEndpointList();

        @JsProperty
        FlightInfo.ToObjectReturnType.FlightDescriptorFieldType getFlightDescriptor();

        @JsProperty
        FlightInfo.ToObjectReturnType.GetSchemaUnionType getSchema();

        @JsProperty
        double getTotalBytes();

        @JsProperty
        double getTotalRecords();

        @JsOverlay
        default void setEndpointList(
                FlightInfo.ToObjectReturnType.EndpointListFieldType[] endpointList) {
            setEndpointList(
                    Js.<JsArray<FlightInfo.ToObjectReturnType.EndpointListFieldType>>uncheckedCast(
                            endpointList));
        }

        @JsProperty
        void setEndpointList(JsArray<FlightInfo.ToObjectReturnType.EndpointListFieldType> endpointList);

        @JsProperty
        void setFlightDescriptor(
                FlightInfo.ToObjectReturnType.FlightDescriptorFieldType flightDescriptor);

        @JsProperty
        void setSchema(FlightInfo.ToObjectReturnType.GetSchemaUnionType schema);

        @JsOverlay
        default void setSchema(String schema) {
            setSchema(Js.<FlightInfo.ToObjectReturnType.GetSchemaUnionType>uncheckedCast(schema));
        }

        @JsOverlay
        default void setSchema(Uint8Array schema) {
            setSchema(Js.<FlightInfo.ToObjectReturnType.GetSchemaUnionType>uncheckedCast(schema));
        }

        @JsProperty
        void setTotalBytes(double totalBytes);

        @JsProperty
        void setTotalRecords(double totalRecords);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface EndpointListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface LocationListFieldType {
                @JsOverlay
                static FlightInfo.ToObjectReturnType0.EndpointListFieldType.LocationListFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getUri();

                @JsProperty
                void setUri(String uri);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TicketFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface GetTicketUnionType {
                    @JsOverlay
                    static FlightInfo.ToObjectReturnType0.EndpointListFieldType.TicketFieldType.GetTicketUnionType of(
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
                static FlightInfo.ToObjectReturnType0.EndpointListFieldType.TicketFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                FlightInfo.ToObjectReturnType0.EndpointListFieldType.TicketFieldType.GetTicketUnionType getTicket();

                @JsProperty
                void setTicket(
                        FlightInfo.ToObjectReturnType0.EndpointListFieldType.TicketFieldType.GetTicketUnionType ticket);

                @JsOverlay
                default void setTicket(String ticket) {
                    setTicket(
                            Js.<FlightInfo.ToObjectReturnType0.EndpointListFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }

                @JsOverlay
                default void setTicket(Uint8Array ticket) {
                    setTicket(
                            Js.<FlightInfo.ToObjectReturnType0.EndpointListFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }
            }

            @JsOverlay
            static FlightInfo.ToObjectReturnType0.EndpointListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<FlightInfo.ToObjectReturnType0.EndpointListFieldType.LocationListFieldType> getLocationList();

            @JsProperty
            FlightInfo.ToObjectReturnType0.EndpointListFieldType.TicketFieldType getTicket();

            @JsProperty
            void setLocationList(
                    JsArray<FlightInfo.ToObjectReturnType0.EndpointListFieldType.LocationListFieldType> locationList);

            @JsOverlay
            default void setLocationList(
                    FlightInfo.ToObjectReturnType0.EndpointListFieldType.LocationListFieldType[] locationList) {
                setLocationList(
                        Js.<JsArray<FlightInfo.ToObjectReturnType0.EndpointListFieldType.LocationListFieldType>>uncheckedCast(
                                locationList));
            }

            @JsProperty
            void setTicket(FlightInfo.ToObjectReturnType0.EndpointListFieldType.TicketFieldType ticket);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface FlightDescriptorFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetCmdUnionType {
                @JsOverlay
                static FlightInfo.ToObjectReturnType0.FlightDescriptorFieldType.GetCmdUnionType of(
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
            static FlightInfo.ToObjectReturnType0.FlightDescriptorFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            FlightInfo.ToObjectReturnType0.FlightDescriptorFieldType.GetCmdUnionType getCmd();

            @JsProperty
            JsArray<String> getPathList();

            @JsProperty
            double getType();

            @JsProperty
            void setCmd(FlightInfo.ToObjectReturnType0.FlightDescriptorFieldType.GetCmdUnionType cmd);

            @JsOverlay
            default void setCmd(String cmd) {
                setCmd(
                        Js.<FlightInfo.ToObjectReturnType0.FlightDescriptorFieldType.GetCmdUnionType>uncheckedCast(
                                cmd));
            }

            @JsOverlay
            default void setCmd(Uint8Array cmd) {
                setCmd(
                        Js.<FlightInfo.ToObjectReturnType0.FlightDescriptorFieldType.GetCmdUnionType>uncheckedCast(
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
        public interface GetSchemaUnionType {
            @JsOverlay
            static FlightInfo.ToObjectReturnType0.GetSchemaUnionType of(Object o) {
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
        static FlightInfo.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<FlightInfo.ToObjectReturnType0.EndpointListFieldType> getEndpointList();

        @JsProperty
        FlightInfo.ToObjectReturnType0.FlightDescriptorFieldType getFlightDescriptor();

        @JsProperty
        FlightInfo.ToObjectReturnType0.GetSchemaUnionType getSchema();

        @JsProperty
        double getTotalBytes();

        @JsProperty
        double getTotalRecords();

        @JsOverlay
        default void setEndpointList(
                FlightInfo.ToObjectReturnType0.EndpointListFieldType[] endpointList) {
            setEndpointList(
                    Js.<JsArray<FlightInfo.ToObjectReturnType0.EndpointListFieldType>>uncheckedCast(
                            endpointList));
        }

        @JsProperty
        void setEndpointList(
                JsArray<FlightInfo.ToObjectReturnType0.EndpointListFieldType> endpointList);

        @JsProperty
        void setFlightDescriptor(
                FlightInfo.ToObjectReturnType0.FlightDescriptorFieldType flightDescriptor);

        @JsProperty
        void setSchema(FlightInfo.ToObjectReturnType0.GetSchemaUnionType schema);

        @JsOverlay
        default void setSchema(String schema) {
            setSchema(Js.<FlightInfo.ToObjectReturnType0.GetSchemaUnionType>uncheckedCast(schema));
        }

        @JsOverlay
        default void setSchema(Uint8Array schema) {
            setSchema(Js.<FlightInfo.ToObjectReturnType0.GetSchemaUnionType>uncheckedCast(schema));
        }

        @JsProperty
        void setTotalBytes(double totalBytes);

        @JsProperty
        void setTotalRecords(double totalRecords);
    }

    public static native FlightInfo deserializeBinary(Uint8Array bytes);

    public static native FlightInfo deserializeBinaryFromReader(FlightInfo message, Object reader);

    public static native void serializeBinaryToWriter(FlightInfo message, Object writer);

    public static native FlightInfo.ToObjectReturnType toObject(
            boolean includeInstance, FlightInfo msg);

    public native FlightEndpoint addEndpoint();

    public native FlightEndpoint addEndpoint(FlightEndpoint value, double index);

    public native FlightEndpoint addEndpoint(FlightEndpoint value);

    public native void clearEndpointList();

    public native void clearFlightDescriptor();

    public native JsArray<FlightEndpoint> getEndpointList();

    public native FlightDescriptor getFlightDescriptor();

    public native FlightInfo.GetSchemaUnionType getSchema();

    public native String getSchema_asB64();

    public native Uint8Array getSchema_asU8();

    public native double getTotalBytes();

    public native double getTotalRecords();

    public native boolean hasFlightDescriptor();

    public native Uint8Array serializeBinary();

    @JsOverlay
    public final void setEndpointList(FlightEndpoint[] value) {
        setEndpointList(Js.<JsArray<FlightEndpoint>>uncheckedCast(value));
    }

    public native void setEndpointList(JsArray<FlightEndpoint> value);

    public native void setFlightDescriptor();

    public native void setFlightDescriptor(FlightDescriptor value);

    public native void setSchema(FlightInfo.SetSchemaValueUnionType value);

    @JsOverlay
    public final void setSchema(String value) {
        setSchema(Js.<FlightInfo.SetSchemaValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public final void setSchema(Uint8Array value) {
        setSchema(Js.<FlightInfo.SetSchemaValueUnionType>uncheckedCast(value));
    }

    public native void setTotalBytes(double value);

    public native void setTotalRecords(double value);

    public native FlightInfo.ToObjectReturnType0 toObject();

    public native FlightInfo.ToObjectReturnType0 toObject(boolean includeInstance);
}
