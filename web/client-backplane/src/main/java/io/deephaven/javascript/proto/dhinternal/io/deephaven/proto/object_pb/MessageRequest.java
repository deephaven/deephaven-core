package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.object_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.object_pb.messagerequest.PayloadCase;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.object_pb.MessageRequest",
        namespace = JsPackage.GLOBAL)
public class MessageRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface DataFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetDataUnionType {
                @JsOverlay
                static MessageRequest.ToObjectReturnType.DataFieldType.GetDataUnionType of(Object o) {
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
            static MessageRequest.ToObjectReturnType.DataFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            MessageRequest.ToObjectReturnType.DataFieldType.GetDataUnionType getData();

            @JsProperty
            JsArray<Object> getTypedExportIdList();

            @JsProperty
            void setData(MessageRequest.ToObjectReturnType.DataFieldType.GetDataUnionType data);

            @JsOverlay
            default void setData(String data) {
                setData(
                        Js.<MessageRequest.ToObjectReturnType.DataFieldType.GetDataUnionType>uncheckedCast(
                                data));
            }

            @JsOverlay
            default void setData(Uint8Array data) {
                setData(
                        Js.<MessageRequest.ToObjectReturnType.DataFieldType.GetDataUnionType>uncheckedCast(
                                data));
            }

            @JsProperty
            void setTypedExportIdList(JsArray<Object> typedExportIdList);

            @JsOverlay
            default void setTypedExportIdList(Object[] typedExportIdList) {
                setTypedExportIdList(Js.<JsArray<Object>>uncheckedCast(typedExportIdList));
            }
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SourceIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TypedTicketFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface TicketFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface GetTicketUnionType {
                        @JsOverlay
                        static MessageRequest.ToObjectReturnType.SourceIdFieldType.TypedTicketFieldType.TicketFieldType.GetTicketUnionType of(
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
                    static MessageRequest.ToObjectReturnType.SourceIdFieldType.TypedTicketFieldType.TicketFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    MessageRequest.ToObjectReturnType.SourceIdFieldType.TypedTicketFieldType.TicketFieldType.GetTicketUnionType getTicket();

                    @JsProperty
                    void setTicket(
                            MessageRequest.ToObjectReturnType.SourceIdFieldType.TypedTicketFieldType.TicketFieldType.GetTicketUnionType ticket);

                    @JsOverlay
                    default void setTicket(String ticket) {
                        setTicket(
                                Js.<MessageRequest.ToObjectReturnType.SourceIdFieldType.TypedTicketFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                        ticket));
                    }

                    @JsOverlay
                    default void setTicket(Uint8Array ticket) {
                        setTicket(
                                Js.<MessageRequest.ToObjectReturnType.SourceIdFieldType.TypedTicketFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                        ticket));
                    }
                }

                @JsOverlay
                static MessageRequest.ToObjectReturnType.SourceIdFieldType.TypedTicketFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                MessageRequest.ToObjectReturnType.SourceIdFieldType.TypedTicketFieldType.TicketFieldType getTicket();

                @JsProperty
                String getType();

                @JsProperty
                void setTicket(
                        MessageRequest.ToObjectReturnType.SourceIdFieldType.TypedTicketFieldType.TicketFieldType ticket);

                @JsProperty
                void setType(String type);
            }

            @JsOverlay
            static MessageRequest.ToObjectReturnType.SourceIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            MessageRequest.ToObjectReturnType.SourceIdFieldType.TypedTicketFieldType getTypedTicket();

            @JsProperty
            void setTypedTicket(
                    MessageRequest.ToObjectReturnType.SourceIdFieldType.TypedTicketFieldType typedTicket);
        }

        @JsOverlay
        static MessageRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        MessageRequest.ToObjectReturnType.DataFieldType getData();

        @JsProperty
        MessageRequest.ToObjectReturnType.SourceIdFieldType getSourceId();

        @JsProperty
        void setData(MessageRequest.ToObjectReturnType.DataFieldType data);

        @JsProperty
        void setSourceId(MessageRequest.ToObjectReturnType.SourceIdFieldType sourceId);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface DataFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetDataUnionType {
                @JsOverlay
                static MessageRequest.ToObjectReturnType0.DataFieldType.GetDataUnionType of(Object o) {
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
            static MessageRequest.ToObjectReturnType0.DataFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            MessageRequest.ToObjectReturnType0.DataFieldType.GetDataUnionType getData();

            @JsProperty
            JsArray<Object> getTypedExportIdList();

            @JsProperty
            void setData(MessageRequest.ToObjectReturnType0.DataFieldType.GetDataUnionType data);

            @JsOverlay
            default void setData(String data) {
                setData(
                        Js.<MessageRequest.ToObjectReturnType0.DataFieldType.GetDataUnionType>uncheckedCast(
                                data));
            }

            @JsOverlay
            default void setData(Uint8Array data) {
                setData(
                        Js.<MessageRequest.ToObjectReturnType0.DataFieldType.GetDataUnionType>uncheckedCast(
                                data));
            }

            @JsProperty
            void setTypedExportIdList(JsArray<Object> typedExportIdList);

            @JsOverlay
            default void setTypedExportIdList(Object[] typedExportIdList) {
                setTypedExportIdList(Js.<JsArray<Object>>uncheckedCast(typedExportIdList));
            }
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SourceIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TypedTicketFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface TicketFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface GetTicketUnionType {
                        @JsOverlay
                        static MessageRequest.ToObjectReturnType0.SourceIdFieldType.TypedTicketFieldType.TicketFieldType.GetTicketUnionType of(
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
                    static MessageRequest.ToObjectReturnType0.SourceIdFieldType.TypedTicketFieldType.TicketFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    MessageRequest.ToObjectReturnType0.SourceIdFieldType.TypedTicketFieldType.TicketFieldType.GetTicketUnionType getTicket();

                    @JsProperty
                    void setTicket(
                            MessageRequest.ToObjectReturnType0.SourceIdFieldType.TypedTicketFieldType.TicketFieldType.GetTicketUnionType ticket);

                    @JsOverlay
                    default void setTicket(String ticket) {
                        setTicket(
                                Js.<MessageRequest.ToObjectReturnType0.SourceIdFieldType.TypedTicketFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                        ticket));
                    }

                    @JsOverlay
                    default void setTicket(Uint8Array ticket) {
                        setTicket(
                                Js.<MessageRequest.ToObjectReturnType0.SourceIdFieldType.TypedTicketFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                        ticket));
                    }
                }

                @JsOverlay
                static MessageRequest.ToObjectReturnType0.SourceIdFieldType.TypedTicketFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                MessageRequest.ToObjectReturnType0.SourceIdFieldType.TypedTicketFieldType.TicketFieldType getTicket();

                @JsProperty
                String getType();

                @JsProperty
                void setTicket(
                        MessageRequest.ToObjectReturnType0.SourceIdFieldType.TypedTicketFieldType.TicketFieldType ticket);

                @JsProperty
                void setType(String type);
            }

            @JsOverlay
            static MessageRequest.ToObjectReturnType0.SourceIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            MessageRequest.ToObjectReturnType0.SourceIdFieldType.TypedTicketFieldType getTypedTicket();

            @JsProperty
            void setTypedTicket(
                    MessageRequest.ToObjectReturnType0.SourceIdFieldType.TypedTicketFieldType typedTicket);
        }

        @JsOverlay
        static MessageRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        MessageRequest.ToObjectReturnType0.DataFieldType getData();

        @JsProperty
        MessageRequest.ToObjectReturnType0.SourceIdFieldType getSourceId();

        @JsProperty
        void setData(MessageRequest.ToObjectReturnType0.DataFieldType data);

        @JsProperty
        void setSourceId(MessageRequest.ToObjectReturnType0.SourceIdFieldType sourceId);
    }

    public static native MessageRequest deserializeBinary(Uint8Array bytes);

    public static native MessageRequest deserializeBinaryFromReader(
            MessageRequest message, Object reader);

    public static native void serializeBinaryToWriter(MessageRequest message, Object writer);

    public static native MessageRequest.ToObjectReturnType toObject(
            boolean includeInstance, MessageRequest msg);

    public native void clearData();

    public native void clearSourceId();

    public native DataRequest getData();

    public native PayloadCase getPayloadCase();

    public native ConnectRequest getSourceId();

    public native boolean hasData();

    public native boolean hasSourceId();

    public native Uint8Array serializeBinary();

    public native void setData();

    public native void setData(DataRequest value);

    public native void setSourceId();

    public native void setSourceId(ConnectRequest value);

    public native MessageRequest.ToObjectReturnType0 toObject();

    public native MessageRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
