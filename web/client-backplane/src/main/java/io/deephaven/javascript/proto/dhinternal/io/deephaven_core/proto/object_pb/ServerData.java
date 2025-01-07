//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.object_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.ticket_pb.TypedTicket;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.object_pb.ServerData",
        namespace = JsPackage.GLOBAL)
public class ServerData {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetPayloadUnionType {
        @JsOverlay
        static ServerData.GetPayloadUnionType of(Object o) {
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
        static ServerData.SetPayloadValueUnionType of(Object o) {
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
        public interface ExportedReferencesListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TicketFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface GetTicketUnionType {
                    @JsOverlay
                    static ServerData.ToObjectReturnType.ExportedReferencesListFieldType.TicketFieldType.GetTicketUnionType of(
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
                static ServerData.ToObjectReturnType.ExportedReferencesListFieldType.TicketFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                ServerData.ToObjectReturnType.ExportedReferencesListFieldType.TicketFieldType.GetTicketUnionType getTicket();

                @JsProperty
                void setTicket(
                        ServerData.ToObjectReturnType.ExportedReferencesListFieldType.TicketFieldType.GetTicketUnionType ticket);

                @JsOverlay
                default void setTicket(String ticket) {
                    setTicket(
                            Js.<ServerData.ToObjectReturnType.ExportedReferencesListFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }

                @JsOverlay
                default void setTicket(Uint8Array ticket) {
                    setTicket(
                            Js.<ServerData.ToObjectReturnType.ExportedReferencesListFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }
            }

            @JsOverlay
            static ServerData.ToObjectReturnType.ExportedReferencesListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            ServerData.ToObjectReturnType.ExportedReferencesListFieldType.TicketFieldType getTicket();

            @JsProperty
            String getType();

            @JsProperty
            void setTicket(
                    ServerData.ToObjectReturnType.ExportedReferencesListFieldType.TicketFieldType ticket);

            @JsProperty
            void setType(String type);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetPayloadUnionType {
            @JsOverlay
            static ServerData.ToObjectReturnType.GetPayloadUnionType of(Object o) {
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
        static ServerData.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<ServerData.ToObjectReturnType.ExportedReferencesListFieldType> getExportedReferencesList();

        @JsProperty
        ServerData.ToObjectReturnType.GetPayloadUnionType getPayload();

        @JsOverlay
        default void setExportedReferencesList(
                ServerData.ToObjectReturnType.ExportedReferencesListFieldType[] exportedReferencesList) {
            setExportedReferencesList(
                    Js.<JsArray<ServerData.ToObjectReturnType.ExportedReferencesListFieldType>>uncheckedCast(
                            exportedReferencesList));
        }

        @JsProperty
        void setExportedReferencesList(
                JsArray<ServerData.ToObjectReturnType.ExportedReferencesListFieldType> exportedReferencesList);

        @JsProperty
        void setPayload(ServerData.ToObjectReturnType.GetPayloadUnionType payload);

        @JsOverlay
        default void setPayload(String payload) {
            setPayload(Js.<ServerData.ToObjectReturnType.GetPayloadUnionType>uncheckedCast(payload));
        }

        @JsOverlay
        default void setPayload(Uint8Array payload) {
            setPayload(Js.<ServerData.ToObjectReturnType.GetPayloadUnionType>uncheckedCast(payload));
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ExportedReferencesListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TicketFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface GetTicketUnionType {
                    @JsOverlay
                    static ServerData.ToObjectReturnType0.ExportedReferencesListFieldType.TicketFieldType.GetTicketUnionType of(
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
                static ServerData.ToObjectReturnType0.ExportedReferencesListFieldType.TicketFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                ServerData.ToObjectReturnType0.ExportedReferencesListFieldType.TicketFieldType.GetTicketUnionType getTicket();

                @JsProperty
                void setTicket(
                        ServerData.ToObjectReturnType0.ExportedReferencesListFieldType.TicketFieldType.GetTicketUnionType ticket);

                @JsOverlay
                default void setTicket(String ticket) {
                    setTicket(
                            Js.<ServerData.ToObjectReturnType0.ExportedReferencesListFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }

                @JsOverlay
                default void setTicket(Uint8Array ticket) {
                    setTicket(
                            Js.<ServerData.ToObjectReturnType0.ExportedReferencesListFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }
            }

            @JsOverlay
            static ServerData.ToObjectReturnType0.ExportedReferencesListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            ServerData.ToObjectReturnType0.ExportedReferencesListFieldType.TicketFieldType getTicket();

            @JsProperty
            String getType();

            @JsProperty
            void setTicket(
                    ServerData.ToObjectReturnType0.ExportedReferencesListFieldType.TicketFieldType ticket);

            @JsProperty
            void setType(String type);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetPayloadUnionType {
            @JsOverlay
            static ServerData.ToObjectReturnType0.GetPayloadUnionType of(Object o) {
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
        static ServerData.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<ServerData.ToObjectReturnType0.ExportedReferencesListFieldType> getExportedReferencesList();

        @JsProperty
        ServerData.ToObjectReturnType0.GetPayloadUnionType getPayload();

        @JsOverlay
        default void setExportedReferencesList(
                ServerData.ToObjectReturnType0.ExportedReferencesListFieldType[] exportedReferencesList) {
            setExportedReferencesList(
                    Js.<JsArray<ServerData.ToObjectReturnType0.ExportedReferencesListFieldType>>uncheckedCast(
                            exportedReferencesList));
        }

        @JsProperty
        void setExportedReferencesList(
                JsArray<ServerData.ToObjectReturnType0.ExportedReferencesListFieldType> exportedReferencesList);

        @JsProperty
        void setPayload(ServerData.ToObjectReturnType0.GetPayloadUnionType payload);

        @JsOverlay
        default void setPayload(String payload) {
            setPayload(Js.<ServerData.ToObjectReturnType0.GetPayloadUnionType>uncheckedCast(payload));
        }

        @JsOverlay
        default void setPayload(Uint8Array payload) {
            setPayload(Js.<ServerData.ToObjectReturnType0.GetPayloadUnionType>uncheckedCast(payload));
        }
    }

    public static native ServerData deserializeBinary(Uint8Array bytes);

    public static native ServerData deserializeBinaryFromReader(ServerData message, Object reader);

    public static native void serializeBinaryToWriter(ServerData message, Object writer);

    public static native ServerData.ToObjectReturnType toObject(
            boolean includeInstance, ServerData msg);

    public native TypedTicket addExportedReferences();

    public native TypedTicket addExportedReferences(TypedTicket value, double index);

    public native TypedTicket addExportedReferences(TypedTicket value);

    public native void clearExportedReferencesList();

    public native JsArray<TypedTicket> getExportedReferencesList();

    public native ServerData.GetPayloadUnionType getPayload();

    public native String getPayload_asB64();

    public native Uint8Array getPayload_asU8();

    public native Uint8Array serializeBinary();

    public native void setExportedReferencesList(JsArray<TypedTicket> value);

    @JsOverlay
    public final void setExportedReferencesList(TypedTicket[] value) {
        setExportedReferencesList(Js.<JsArray<TypedTicket>>uncheckedCast(value));
    }

    public native void setPayload(ServerData.SetPayloadValueUnionType value);

    @JsOverlay
    public final void setPayload(String value) {
        setPayload(Js.<ServerData.SetPayloadValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public final void setPayload(Uint8Array value) {
        setPayload(Js.<ServerData.SetPayloadValueUnionType>uncheckedCast(value));
    }

    public native ServerData.ToObjectReturnType0 toObject();

    public native ServerData.ToObjectReturnType0 toObject(boolean includeInstance);
}
