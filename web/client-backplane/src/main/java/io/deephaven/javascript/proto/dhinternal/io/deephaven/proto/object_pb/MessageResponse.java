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
        name = "dhinternal.io.deephaven.proto.object_pb.MessageResponse",
        namespace = JsPackage.GLOBAL)
public class MessageResponse {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetDataUnionType {
        @JsOverlay
        static MessageResponse.GetDataUnionType of(Object o) {
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
    public interface SetDataValueUnionType {
        @JsOverlay
        static MessageResponse.SetDataValueUnionType of(Object o) {
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
        public interface GetDataUnionType {
            @JsOverlay
            static MessageResponse.ToObjectReturnType.GetDataUnionType of(Object o) {
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
        public interface TypedExportIdListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TicketFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface GetTicketUnionType {
                    @JsOverlay
                    static MessageResponse.ToObjectReturnType.TypedExportIdListFieldType.TicketFieldType.GetTicketUnionType of(
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
                static MessageResponse.ToObjectReturnType.TypedExportIdListFieldType.TicketFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                MessageResponse.ToObjectReturnType.TypedExportIdListFieldType.TicketFieldType.GetTicketUnionType getTicket();

                @JsProperty
                void setTicket(
                        MessageResponse.ToObjectReturnType.TypedExportIdListFieldType.TicketFieldType.GetTicketUnionType ticket);

                @JsOverlay
                default void setTicket(String ticket) {
                    setTicket(
                            Js.<MessageResponse.ToObjectReturnType.TypedExportIdListFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }

                @JsOverlay
                default void setTicket(Uint8Array ticket) {
                    setTicket(
                            Js.<MessageResponse.ToObjectReturnType.TypedExportIdListFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }
            }

            @JsOverlay
            static MessageResponse.ToObjectReturnType.TypedExportIdListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            MessageResponse.ToObjectReturnType.TypedExportIdListFieldType.TicketFieldType getTicket();

            @JsProperty
            String getType();

            @JsProperty
            void setTicket(
                    MessageResponse.ToObjectReturnType.TypedExportIdListFieldType.TicketFieldType ticket);

            @JsProperty
            void setType(String type);
        }

        @JsOverlay
        static MessageResponse.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        MessageResponse.ToObjectReturnType.GetDataUnionType getData();

        @JsProperty
        JsArray<MessageResponse.ToObjectReturnType.TypedExportIdListFieldType> getTypedExportIdList();

        @JsProperty
        void setData(MessageResponse.ToObjectReturnType.GetDataUnionType data);

        @JsOverlay
        default void setData(String data) {
            setData(Js.<MessageResponse.ToObjectReturnType.GetDataUnionType>uncheckedCast(data));
        }

        @JsOverlay
        default void setData(Uint8Array data) {
            setData(Js.<MessageResponse.ToObjectReturnType.GetDataUnionType>uncheckedCast(data));
        }

        @JsProperty
        void setTypedExportIdList(
                JsArray<MessageResponse.ToObjectReturnType.TypedExportIdListFieldType> typedExportIdList);

        @JsOverlay
        default void setTypedExportIdList(
                MessageResponse.ToObjectReturnType.TypedExportIdListFieldType[] typedExportIdList) {
            setTypedExportIdList(
                    Js.<JsArray<MessageResponse.ToObjectReturnType.TypedExportIdListFieldType>>uncheckedCast(
                            typedExportIdList));
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetDataUnionType {
            @JsOverlay
            static MessageResponse.ToObjectReturnType0.GetDataUnionType of(Object o) {
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
        public interface TypedExportIdListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TicketFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface GetTicketUnionType {
                    @JsOverlay
                    static MessageResponse.ToObjectReturnType0.TypedExportIdListFieldType.TicketFieldType.GetTicketUnionType of(
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
                static MessageResponse.ToObjectReturnType0.TypedExportIdListFieldType.TicketFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                MessageResponse.ToObjectReturnType0.TypedExportIdListFieldType.TicketFieldType.GetTicketUnionType getTicket();

                @JsProperty
                void setTicket(
                        MessageResponse.ToObjectReturnType0.TypedExportIdListFieldType.TicketFieldType.GetTicketUnionType ticket);

                @JsOverlay
                default void setTicket(String ticket) {
                    setTicket(
                            Js.<MessageResponse.ToObjectReturnType0.TypedExportIdListFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }

                @JsOverlay
                default void setTicket(Uint8Array ticket) {
                    setTicket(
                            Js.<MessageResponse.ToObjectReturnType0.TypedExportIdListFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }
            }

            @JsOverlay
            static MessageResponse.ToObjectReturnType0.TypedExportIdListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            MessageResponse.ToObjectReturnType0.TypedExportIdListFieldType.TicketFieldType getTicket();

            @JsProperty
            String getType();

            @JsProperty
            void setTicket(
                    MessageResponse.ToObjectReturnType0.TypedExportIdListFieldType.TicketFieldType ticket);

            @JsProperty
            void setType(String type);
        }

        @JsOverlay
        static MessageResponse.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        MessageResponse.ToObjectReturnType0.GetDataUnionType getData();

        @JsProperty
        JsArray<MessageResponse.ToObjectReturnType0.TypedExportIdListFieldType> getTypedExportIdList();

        @JsProperty
        void setData(MessageResponse.ToObjectReturnType0.GetDataUnionType data);

        @JsOverlay
        default void setData(String data) {
            setData(Js.<MessageResponse.ToObjectReturnType0.GetDataUnionType>uncheckedCast(data));
        }

        @JsOverlay
        default void setData(Uint8Array data) {
            setData(Js.<MessageResponse.ToObjectReturnType0.GetDataUnionType>uncheckedCast(data));
        }

        @JsProperty
        void setTypedExportIdList(
                JsArray<MessageResponse.ToObjectReturnType0.TypedExportIdListFieldType> typedExportIdList);

        @JsOverlay
        default void setTypedExportIdList(
                MessageResponse.ToObjectReturnType0.TypedExportIdListFieldType[] typedExportIdList) {
            setTypedExportIdList(
                    Js.<JsArray<MessageResponse.ToObjectReturnType0.TypedExportIdListFieldType>>uncheckedCast(
                            typedExportIdList));
        }
    }

    public static native MessageResponse deserializeBinary(Uint8Array bytes);

    public static native MessageResponse deserializeBinaryFromReader(
            MessageResponse message, Object reader);

    public static native void serializeBinaryToWriter(MessageResponse message, Object writer);

    public static native MessageResponse.ToObjectReturnType toObject(
            boolean includeInstance, MessageResponse msg);

    public native TypedTicket addTypedExportId();

    public native TypedTicket addTypedExportId(TypedTicket value, double index);

    public native TypedTicket addTypedExportId(TypedTicket value);

    public native void clearTypedExportIdList();

    public native MessageResponse.GetDataUnionType getData();

    public native String getData_asB64();

    public native Uint8Array getData_asU8();

    public native JsArray<TypedTicket> getTypedExportIdList();

    public native Uint8Array serializeBinary();

    public native void setData(MessageResponse.SetDataValueUnionType value);

    @JsOverlay
    public final void setData(String value) {
        setData(Js.<MessageResponse.SetDataValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public final void setData(Uint8Array value) {
        setData(Js.<MessageResponse.SetDataValueUnionType>uncheckedCast(value));
    }

    public native void setTypedExportIdList(JsArray<TypedTicket> value);

    @JsOverlay
    public final void setTypedExportIdList(TypedTicket[] value) {
        setTypedExportIdList(Js.<JsArray<TypedTicket>>uncheckedCast(value));
    }

    public native MessageResponse.ToObjectReturnType0 toObject();

    public native MessageResponse.ToObjectReturnType0 toObject(boolean includeInstance);
}
