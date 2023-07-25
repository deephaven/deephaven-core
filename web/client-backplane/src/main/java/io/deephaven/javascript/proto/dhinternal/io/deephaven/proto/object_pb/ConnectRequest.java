/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.object_pb;

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
        name = "dhinternal.io.deephaven.proto.object_pb.ConnectRequest",
        namespace = JsPackage.GLOBAL)
public class ConnectRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface TypedTicketFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TicketFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface GetTicketUnionType {
                    @JsOverlay
                    static ConnectRequest.ToObjectReturnType.TypedTicketFieldType.TicketFieldType.GetTicketUnionType of(
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
                static ConnectRequest.ToObjectReturnType.TypedTicketFieldType.TicketFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                ConnectRequest.ToObjectReturnType.TypedTicketFieldType.TicketFieldType.GetTicketUnionType getTicket();

                @JsProperty
                void setTicket(
                        ConnectRequest.ToObjectReturnType.TypedTicketFieldType.TicketFieldType.GetTicketUnionType ticket);

                @JsOverlay
                default void setTicket(String ticket) {
                    setTicket(
                            Js.<ConnectRequest.ToObjectReturnType.TypedTicketFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }

                @JsOverlay
                default void setTicket(Uint8Array ticket) {
                    setTicket(
                            Js.<ConnectRequest.ToObjectReturnType.TypedTicketFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }
            }

            @JsOverlay
            static ConnectRequest.ToObjectReturnType.TypedTicketFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            ConnectRequest.ToObjectReturnType.TypedTicketFieldType.TicketFieldType getTicket();

            @JsProperty
            String getType();

            @JsProperty
            void setTicket(ConnectRequest.ToObjectReturnType.TypedTicketFieldType.TicketFieldType ticket);

            @JsProperty
            void setType(String type);
        }

        @JsOverlay
        static ConnectRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        ConnectRequest.ToObjectReturnType.TypedTicketFieldType getTypedTicket();

        @JsProperty
        void setTypedTicket(ConnectRequest.ToObjectReturnType.TypedTicketFieldType typedTicket);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface TypedTicketFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TicketFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface GetTicketUnionType {
                    @JsOverlay
                    static ConnectRequest.ToObjectReturnType0.TypedTicketFieldType.TicketFieldType.GetTicketUnionType of(
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
                static ConnectRequest.ToObjectReturnType0.TypedTicketFieldType.TicketFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                ConnectRequest.ToObjectReturnType0.TypedTicketFieldType.TicketFieldType.GetTicketUnionType getTicket();

                @JsProperty
                void setTicket(
                        ConnectRequest.ToObjectReturnType0.TypedTicketFieldType.TicketFieldType.GetTicketUnionType ticket);

                @JsOverlay
                default void setTicket(String ticket) {
                    setTicket(
                            Js.<ConnectRequest.ToObjectReturnType0.TypedTicketFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }

                @JsOverlay
                default void setTicket(Uint8Array ticket) {
                    setTicket(
                            Js.<ConnectRequest.ToObjectReturnType0.TypedTicketFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }
            }

            @JsOverlay
            static ConnectRequest.ToObjectReturnType0.TypedTicketFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            ConnectRequest.ToObjectReturnType0.TypedTicketFieldType.TicketFieldType getTicket();

            @JsProperty
            String getType();

            @JsProperty
            void setTicket(
                    ConnectRequest.ToObjectReturnType0.TypedTicketFieldType.TicketFieldType ticket);

            @JsProperty
            void setType(String type);
        }

        @JsOverlay
        static ConnectRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        ConnectRequest.ToObjectReturnType0.TypedTicketFieldType getTypedTicket();

        @JsProperty
        void setTypedTicket(ConnectRequest.ToObjectReturnType0.TypedTicketFieldType typedTicket);
    }

    public static native ConnectRequest deserializeBinary(Uint8Array bytes);

    public static native ConnectRequest deserializeBinaryFromReader(
            ConnectRequest message, Object reader);

    public static native void serializeBinaryToWriter(ConnectRequest message, Object writer);

    public static native ConnectRequest.ToObjectReturnType toObject(
            boolean includeInstance, ConnectRequest msg);

    public native void clearTypedTicket();

    public native TypedTicket getTypedTicket();

    public native boolean hasTypedTicket();

    public native Uint8Array serializeBinary();

    public native void setTypedTicket();

    public native void setTypedTicket(TypedTicket value);

    public native ConnectRequest.ToObjectReturnType0 toObject();

    public native ConnectRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
