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
        name = "dhinternal.io.deephaven_core.proto.object_pb.ClientData",
        namespace = JsPackage.GLOBAL)
public class ClientData {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetPayloadUnionType {
        @JsOverlay
        static ClientData.GetPayloadUnionType of(Object o) {
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
        static ClientData.SetPayloadValueUnionType of(Object o) {
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
            static ClientData.ToObjectReturnType.GetPayloadUnionType of(Object o) {
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
        public interface ReferencesListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TicketFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface GetTicketUnionType {
                    @JsOverlay
                    static ClientData.ToObjectReturnType.ReferencesListFieldType.TicketFieldType.GetTicketUnionType of(
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
                static ClientData.ToObjectReturnType.ReferencesListFieldType.TicketFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                ClientData.ToObjectReturnType.ReferencesListFieldType.TicketFieldType.GetTicketUnionType getTicket();

                @JsProperty
                void setTicket(
                        ClientData.ToObjectReturnType.ReferencesListFieldType.TicketFieldType.GetTicketUnionType ticket);

                @JsOverlay
                default void setTicket(String ticket) {
                    setTicket(
                            Js.<ClientData.ToObjectReturnType.ReferencesListFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }

                @JsOverlay
                default void setTicket(Uint8Array ticket) {
                    setTicket(
                            Js.<ClientData.ToObjectReturnType.ReferencesListFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }
            }

            @JsOverlay
            static ClientData.ToObjectReturnType.ReferencesListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            ClientData.ToObjectReturnType.ReferencesListFieldType.TicketFieldType getTicket();

            @JsProperty
            String getType();

            @JsProperty
            void setTicket(ClientData.ToObjectReturnType.ReferencesListFieldType.TicketFieldType ticket);

            @JsProperty
            void setType(String type);
        }

        @JsOverlay
        static ClientData.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        ClientData.ToObjectReturnType.GetPayloadUnionType getPayload();

        @JsProperty
        JsArray<ClientData.ToObjectReturnType.ReferencesListFieldType> getReferencesList();

        @JsProperty
        void setPayload(ClientData.ToObjectReturnType.GetPayloadUnionType payload);

        @JsOverlay
        default void setPayload(String payload) {
            setPayload(Js.<ClientData.ToObjectReturnType.GetPayloadUnionType>uncheckedCast(payload));
        }

        @JsOverlay
        default void setPayload(Uint8Array payload) {
            setPayload(Js.<ClientData.ToObjectReturnType.GetPayloadUnionType>uncheckedCast(payload));
        }

        @JsProperty
        void setReferencesList(
                JsArray<ClientData.ToObjectReturnType.ReferencesListFieldType> referencesList);

        @JsOverlay
        default void setReferencesList(
                ClientData.ToObjectReturnType.ReferencesListFieldType[] referencesList) {
            setReferencesList(
                    Js.<JsArray<ClientData.ToObjectReturnType.ReferencesListFieldType>>uncheckedCast(
                            referencesList));
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetPayloadUnionType {
            @JsOverlay
            static ClientData.ToObjectReturnType0.GetPayloadUnionType of(Object o) {
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
        public interface ReferencesListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TicketFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface GetTicketUnionType {
                    @JsOverlay
                    static ClientData.ToObjectReturnType0.ReferencesListFieldType.TicketFieldType.GetTicketUnionType of(
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
                static ClientData.ToObjectReturnType0.ReferencesListFieldType.TicketFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                ClientData.ToObjectReturnType0.ReferencesListFieldType.TicketFieldType.GetTicketUnionType getTicket();

                @JsProperty
                void setTicket(
                        ClientData.ToObjectReturnType0.ReferencesListFieldType.TicketFieldType.GetTicketUnionType ticket);

                @JsOverlay
                default void setTicket(String ticket) {
                    setTicket(
                            Js.<ClientData.ToObjectReturnType0.ReferencesListFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }

                @JsOverlay
                default void setTicket(Uint8Array ticket) {
                    setTicket(
                            Js.<ClientData.ToObjectReturnType0.ReferencesListFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }
            }

            @JsOverlay
            static ClientData.ToObjectReturnType0.ReferencesListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            ClientData.ToObjectReturnType0.ReferencesListFieldType.TicketFieldType getTicket();

            @JsProperty
            String getType();

            @JsProperty
            void setTicket(ClientData.ToObjectReturnType0.ReferencesListFieldType.TicketFieldType ticket);

            @JsProperty
            void setType(String type);
        }

        @JsOverlay
        static ClientData.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        ClientData.ToObjectReturnType0.GetPayloadUnionType getPayload();

        @JsProperty
        JsArray<ClientData.ToObjectReturnType0.ReferencesListFieldType> getReferencesList();

        @JsProperty
        void setPayload(ClientData.ToObjectReturnType0.GetPayloadUnionType payload);

        @JsOverlay
        default void setPayload(String payload) {
            setPayload(Js.<ClientData.ToObjectReturnType0.GetPayloadUnionType>uncheckedCast(payload));
        }

        @JsOverlay
        default void setPayload(Uint8Array payload) {
            setPayload(Js.<ClientData.ToObjectReturnType0.GetPayloadUnionType>uncheckedCast(payload));
        }

        @JsProperty
        void setReferencesList(
                JsArray<ClientData.ToObjectReturnType0.ReferencesListFieldType> referencesList);

        @JsOverlay
        default void setReferencesList(
                ClientData.ToObjectReturnType0.ReferencesListFieldType[] referencesList) {
            setReferencesList(
                    Js.<JsArray<ClientData.ToObjectReturnType0.ReferencesListFieldType>>uncheckedCast(
                            referencesList));
        }
    }

    public static native ClientData deserializeBinary(Uint8Array bytes);

    public static native ClientData deserializeBinaryFromReader(ClientData message, Object reader);

    public static native void serializeBinaryToWriter(ClientData message, Object writer);

    public static native ClientData.ToObjectReturnType toObject(
            boolean includeInstance, ClientData msg);

    public native TypedTicket addReferences();

    public native TypedTicket addReferences(TypedTicket value, double index);

    public native TypedTicket addReferences(TypedTicket value);

    public native void clearReferencesList();

    public native ClientData.GetPayloadUnionType getPayload();

    public native String getPayload_asB64();

    public native Uint8Array getPayload_asU8();

    public native JsArray<TypedTicket> getReferencesList();

    public native Uint8Array serializeBinary();

    public native void setPayload(ClientData.SetPayloadValueUnionType value);

    @JsOverlay
    public final void setPayload(String value) {
        setPayload(Js.<ClientData.SetPayloadValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public final void setPayload(Uint8Array value) {
        setPayload(Js.<ClientData.SetPayloadValueUnionType>uncheckedCast(value));
    }

    public native void setReferencesList(JsArray<TypedTicket> value);

    @JsOverlay
    public final void setReferencesList(TypedTicket[] value) {
        setReferencesList(Js.<JsArray<TypedTicket>>uncheckedCast(value));
    }

    public native ClientData.ToObjectReturnType0 toObject();

    public native ClientData.ToObjectReturnType0 toObject(boolean includeInstance);
}
