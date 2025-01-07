//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.object_pb;

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
        name = "dhinternal.io.deephaven_core.proto.object_pb.StreamRequest",
        namespace = JsPackage.GLOBAL)
public class StreamRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ConnectFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface SourceIdFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface TicketFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface GetTicketUnionType {
                        @JsOverlay
                        static StreamRequest.ToObjectReturnType.ConnectFieldType.SourceIdFieldType.TicketFieldType.GetTicketUnionType of(
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
                    static StreamRequest.ToObjectReturnType.ConnectFieldType.SourceIdFieldType.TicketFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    StreamRequest.ToObjectReturnType.ConnectFieldType.SourceIdFieldType.TicketFieldType.GetTicketUnionType getTicket();

                    @JsProperty
                    void setTicket(
                            StreamRequest.ToObjectReturnType.ConnectFieldType.SourceIdFieldType.TicketFieldType.GetTicketUnionType ticket);

                    @JsOverlay
                    default void setTicket(String ticket) {
                        setTicket(
                                Js.<StreamRequest.ToObjectReturnType.ConnectFieldType.SourceIdFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                        ticket));
                    }

                    @JsOverlay
                    default void setTicket(Uint8Array ticket) {
                        setTicket(
                                Js.<StreamRequest.ToObjectReturnType.ConnectFieldType.SourceIdFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                        ticket));
                    }
                }

                @JsOverlay
                static StreamRequest.ToObjectReturnType.ConnectFieldType.SourceIdFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                StreamRequest.ToObjectReturnType.ConnectFieldType.SourceIdFieldType.TicketFieldType getTicket();

                @JsProperty
                String getType();

                @JsProperty
                void setTicket(
                        StreamRequest.ToObjectReturnType.ConnectFieldType.SourceIdFieldType.TicketFieldType ticket);

                @JsProperty
                void setType(String type);
            }

            @JsOverlay
            static StreamRequest.ToObjectReturnType.ConnectFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            StreamRequest.ToObjectReturnType.ConnectFieldType.SourceIdFieldType getSourceId();

            @JsProperty
            void setSourceId(
                    StreamRequest.ToObjectReturnType.ConnectFieldType.SourceIdFieldType sourceId);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface DataFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetPayloadUnionType {
                @JsOverlay
                static StreamRequest.ToObjectReturnType.DataFieldType.GetPayloadUnionType of(Object o) {
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
            static StreamRequest.ToObjectReturnType.DataFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            StreamRequest.ToObjectReturnType.DataFieldType.GetPayloadUnionType getPayload();

            @JsProperty
            JsArray<Object> getReferencesList();

            @JsProperty
            void setPayload(StreamRequest.ToObjectReturnType.DataFieldType.GetPayloadUnionType payload);

            @JsOverlay
            default void setPayload(String payload) {
                setPayload(
                        Js.<StreamRequest.ToObjectReturnType.DataFieldType.GetPayloadUnionType>uncheckedCast(
                                payload));
            }

            @JsOverlay
            default void setPayload(Uint8Array payload) {
                setPayload(
                        Js.<StreamRequest.ToObjectReturnType.DataFieldType.GetPayloadUnionType>uncheckedCast(
                                payload));
            }

            @JsProperty
            void setReferencesList(JsArray<Object> referencesList);

            @JsOverlay
            default void setReferencesList(Object[] referencesList) {
                setReferencesList(Js.<JsArray<Object>>uncheckedCast(referencesList));
            }
        }

        @JsOverlay
        static StreamRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        StreamRequest.ToObjectReturnType.ConnectFieldType getConnect();

        @JsProperty
        StreamRequest.ToObjectReturnType.DataFieldType getData();

        @JsProperty
        void setConnect(StreamRequest.ToObjectReturnType.ConnectFieldType connect);

        @JsProperty
        void setData(StreamRequest.ToObjectReturnType.DataFieldType data);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ConnectFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface SourceIdFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface TicketFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface GetTicketUnionType {
                        @JsOverlay
                        static StreamRequest.ToObjectReturnType0.ConnectFieldType.SourceIdFieldType.TicketFieldType.GetTicketUnionType of(
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
                    static StreamRequest.ToObjectReturnType0.ConnectFieldType.SourceIdFieldType.TicketFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    StreamRequest.ToObjectReturnType0.ConnectFieldType.SourceIdFieldType.TicketFieldType.GetTicketUnionType getTicket();

                    @JsProperty
                    void setTicket(
                            StreamRequest.ToObjectReturnType0.ConnectFieldType.SourceIdFieldType.TicketFieldType.GetTicketUnionType ticket);

                    @JsOverlay
                    default void setTicket(String ticket) {
                        setTicket(
                                Js.<StreamRequest.ToObjectReturnType0.ConnectFieldType.SourceIdFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                        ticket));
                    }

                    @JsOverlay
                    default void setTicket(Uint8Array ticket) {
                        setTicket(
                                Js.<StreamRequest.ToObjectReturnType0.ConnectFieldType.SourceIdFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                        ticket));
                    }
                }

                @JsOverlay
                static StreamRequest.ToObjectReturnType0.ConnectFieldType.SourceIdFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                StreamRequest.ToObjectReturnType0.ConnectFieldType.SourceIdFieldType.TicketFieldType getTicket();

                @JsProperty
                String getType();

                @JsProperty
                void setTicket(
                        StreamRequest.ToObjectReturnType0.ConnectFieldType.SourceIdFieldType.TicketFieldType ticket);

                @JsProperty
                void setType(String type);
            }

            @JsOverlay
            static StreamRequest.ToObjectReturnType0.ConnectFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            StreamRequest.ToObjectReturnType0.ConnectFieldType.SourceIdFieldType getSourceId();

            @JsProperty
            void setSourceId(
                    StreamRequest.ToObjectReturnType0.ConnectFieldType.SourceIdFieldType sourceId);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface DataFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetPayloadUnionType {
                @JsOverlay
                static StreamRequest.ToObjectReturnType0.DataFieldType.GetPayloadUnionType of(Object o) {
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
            static StreamRequest.ToObjectReturnType0.DataFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            StreamRequest.ToObjectReturnType0.DataFieldType.GetPayloadUnionType getPayload();

            @JsProperty
            JsArray<Object> getReferencesList();

            @JsProperty
            void setPayload(StreamRequest.ToObjectReturnType0.DataFieldType.GetPayloadUnionType payload);

            @JsOverlay
            default void setPayload(String payload) {
                setPayload(
                        Js.<StreamRequest.ToObjectReturnType0.DataFieldType.GetPayloadUnionType>uncheckedCast(
                                payload));
            }

            @JsOverlay
            default void setPayload(Uint8Array payload) {
                setPayload(
                        Js.<StreamRequest.ToObjectReturnType0.DataFieldType.GetPayloadUnionType>uncheckedCast(
                                payload));
            }

            @JsProperty
            void setReferencesList(JsArray<Object> referencesList);

            @JsOverlay
            default void setReferencesList(Object[] referencesList) {
                setReferencesList(Js.<JsArray<Object>>uncheckedCast(referencesList));
            }
        }

        @JsOverlay
        static StreamRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        StreamRequest.ToObjectReturnType0.ConnectFieldType getConnect();

        @JsProperty
        StreamRequest.ToObjectReturnType0.DataFieldType getData();

        @JsProperty
        void setConnect(StreamRequest.ToObjectReturnType0.ConnectFieldType connect);

        @JsProperty
        void setData(StreamRequest.ToObjectReturnType0.DataFieldType data);
    }

    public static native StreamRequest deserializeBinary(Uint8Array bytes);

    public static native StreamRequest deserializeBinaryFromReader(
            StreamRequest message, Object reader);

    public static native void serializeBinaryToWriter(StreamRequest message, Object writer);

    public static native StreamRequest.ToObjectReturnType toObject(
            boolean includeInstance, StreamRequest msg);

    public native void clearConnect();

    public native void clearData();

    public native ConnectRequest getConnect();

    public native ClientData getData();

    public native int getMessageCase();

    public native boolean hasConnect();

    public native boolean hasData();

    public native Uint8Array serializeBinary();

    public native void setConnect();

    public native void setConnect(ConnectRequest value);

    public native void setData();

    public native void setData(ClientData value);

    public native StreamRequest.ToObjectReturnType0 toObject();

    public native StreamRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
