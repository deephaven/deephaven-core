/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.object_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.TypedTicket;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.object_pb.Data",
        namespace = JsPackage.GLOBAL)
public class Data {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetPayloadUnionType {
        @JsOverlay
        static Data.GetPayloadUnionType of(Object o) {
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
        static Data.SetPayloadValueUnionType of(Object o) {
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
            static Data.ToObjectReturnType.GetPayloadUnionType of(Object o) {
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
        public interface TypedExportIdsListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TicketFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface GetTicketUnionType {
                    @JsOverlay
                    static Data.ToObjectReturnType.TypedExportIdsListFieldType.TicketFieldType.GetTicketUnionType of(
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
                static Data.ToObjectReturnType.TypedExportIdsListFieldType.TicketFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Data.ToObjectReturnType.TypedExportIdsListFieldType.TicketFieldType.GetTicketUnionType getTicket();

                @JsProperty
                void setTicket(
                        Data.ToObjectReturnType.TypedExportIdsListFieldType.TicketFieldType.GetTicketUnionType ticket);

                @JsOverlay
                default void setTicket(String ticket) {
                    setTicket(
                            Js.<Data.ToObjectReturnType.TypedExportIdsListFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }

                @JsOverlay
                default void setTicket(Uint8Array ticket) {
                    setTicket(
                            Js.<Data.ToObjectReturnType.TypedExportIdsListFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }
            }

            @JsOverlay
            static Data.ToObjectReturnType.TypedExportIdsListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            Data.ToObjectReturnType.TypedExportIdsListFieldType.TicketFieldType getTicket();

            @JsProperty
            String getType();

            @JsProperty
            void setTicket(Data.ToObjectReturnType.TypedExportIdsListFieldType.TicketFieldType ticket);

            @JsProperty
            void setType(String type);
        }

        @JsOverlay
        static Data.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Data.ToObjectReturnType.GetPayloadUnionType getPayload();

        @JsProperty
        JsArray<Data.ToObjectReturnType.TypedExportIdsListFieldType> getTypedExportIdsList();

        @JsProperty
        void setPayload(Data.ToObjectReturnType.GetPayloadUnionType payload);

        @JsOverlay
        default void setPayload(String payload) {
            setPayload(Js.<Data.ToObjectReturnType.GetPayloadUnionType>uncheckedCast(payload));
        }

        @JsOverlay
        default void setPayload(Uint8Array payload) {
            setPayload(Js.<Data.ToObjectReturnType.GetPayloadUnionType>uncheckedCast(payload));
        }

        @JsProperty
        void setTypedExportIdsList(
                JsArray<Data.ToObjectReturnType.TypedExportIdsListFieldType> typedExportIdsList);

        @JsOverlay
        default void setTypedExportIdsList(
                Data.ToObjectReturnType.TypedExportIdsListFieldType[] typedExportIdsList) {
            setTypedExportIdsList(
                    Js.<JsArray<Data.ToObjectReturnType.TypedExportIdsListFieldType>>uncheckedCast(
                            typedExportIdsList));
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetPayloadUnionType {
            @JsOverlay
            static Data.ToObjectReturnType0.GetPayloadUnionType of(Object o) {
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
        public interface TypedExportIdsListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TicketFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface GetTicketUnionType {
                    @JsOverlay
                    static Data.ToObjectReturnType0.TypedExportIdsListFieldType.TicketFieldType.GetTicketUnionType of(
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
                static Data.ToObjectReturnType0.TypedExportIdsListFieldType.TicketFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Data.ToObjectReturnType0.TypedExportIdsListFieldType.TicketFieldType.GetTicketUnionType getTicket();

                @JsProperty
                void setTicket(
                        Data.ToObjectReturnType0.TypedExportIdsListFieldType.TicketFieldType.GetTicketUnionType ticket);

                @JsOverlay
                default void setTicket(String ticket) {
                    setTicket(
                            Js.<Data.ToObjectReturnType0.TypedExportIdsListFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }

                @JsOverlay
                default void setTicket(Uint8Array ticket) {
                    setTicket(
                            Js.<Data.ToObjectReturnType0.TypedExportIdsListFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }
            }

            @JsOverlay
            static Data.ToObjectReturnType0.TypedExportIdsListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            Data.ToObjectReturnType0.TypedExportIdsListFieldType.TicketFieldType getTicket();

            @JsProperty
            String getType();

            @JsProperty
            void setTicket(Data.ToObjectReturnType0.TypedExportIdsListFieldType.TicketFieldType ticket);

            @JsProperty
            void setType(String type);
        }

        @JsOverlay
        static Data.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Data.ToObjectReturnType0.GetPayloadUnionType getPayload();

        @JsProperty
        JsArray<Data.ToObjectReturnType0.TypedExportIdsListFieldType> getTypedExportIdsList();

        @JsProperty
        void setPayload(Data.ToObjectReturnType0.GetPayloadUnionType payload);

        @JsOverlay
        default void setPayload(String payload) {
            setPayload(Js.<Data.ToObjectReturnType0.GetPayloadUnionType>uncheckedCast(payload));
        }

        @JsOverlay
        default void setPayload(Uint8Array payload) {
            setPayload(Js.<Data.ToObjectReturnType0.GetPayloadUnionType>uncheckedCast(payload));
        }

        @JsProperty
        void setTypedExportIdsList(
                JsArray<Data.ToObjectReturnType0.TypedExportIdsListFieldType> typedExportIdsList);

        @JsOverlay
        default void setTypedExportIdsList(
                Data.ToObjectReturnType0.TypedExportIdsListFieldType[] typedExportIdsList) {
            setTypedExportIdsList(
                    Js.<JsArray<Data.ToObjectReturnType0.TypedExportIdsListFieldType>>uncheckedCast(
                            typedExportIdsList));
        }
    }

    public static native Data deserializeBinary(Uint8Array bytes);

    public static native Data deserializeBinaryFromReader(Data message, Object reader);

    public static native void serializeBinaryToWriter(Data message, Object writer);

    public static native Data.ToObjectReturnType toObject(boolean includeInstance, Data msg);

    public native TypedTicket addTypedExportIds();

    public native TypedTicket addTypedExportIds(TypedTicket value, double index);

    public native TypedTicket addTypedExportIds(TypedTicket value);

    public native void clearTypedExportIdsList();

    public native Data.GetPayloadUnionType getPayload();

    public native String getPayload_asB64();

    public native Uint8Array getPayload_asU8();

    public native JsArray<TypedTicket> getTypedExportIdsList();

    public native Uint8Array serializeBinary();

    public native void setPayload(Data.SetPayloadValueUnionType value);

    @JsOverlay
    public final void setPayload(String value) {
        setPayload(Js.<Data.SetPayloadValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public final void setPayload(Uint8Array value) {
        setPayload(Js.<Data.SetPayloadValueUnionType>uncheckedCast(value));
    }

    public native void setTypedExportIdsList(JsArray<TypedTicket> value);

    @JsOverlay
    public final void setTypedExportIdsList(TypedTicket[] value) {
        setTypedExportIdsList(Js.<JsArray<TypedTicket>>uncheckedCast(value));
    }

    public native Data.ToObjectReturnType0 toObject();

    public native Data.ToObjectReturnType0 toObject(boolean includeInstance);
}
