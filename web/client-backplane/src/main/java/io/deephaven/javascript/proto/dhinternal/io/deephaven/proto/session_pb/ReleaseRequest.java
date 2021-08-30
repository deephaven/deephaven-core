package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb;

import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.Ticket;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.session_pb.ReleaseRequest",
        namespace = JsPackage.GLOBAL)
public class ReleaseRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface IdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static ReleaseRequest.ToObjectReturnType.IdFieldType.GetTicketUnionType of(Object o) {
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
            static ReleaseRequest.ToObjectReturnType.IdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            ReleaseRequest.ToObjectReturnType.IdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(ReleaseRequest.ToObjectReturnType.IdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<ReleaseRequest.ToObjectReturnType.IdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<ReleaseRequest.ToObjectReturnType.IdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsOverlay
        static ReleaseRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        ReleaseRequest.ToObjectReturnType.IdFieldType getId();

        @JsProperty
        void setId(ReleaseRequest.ToObjectReturnType.IdFieldType id);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface IdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static ReleaseRequest.ToObjectReturnType0.IdFieldType.GetTicketUnionType of(Object o) {
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
            static ReleaseRequest.ToObjectReturnType0.IdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            ReleaseRequest.ToObjectReturnType0.IdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(ReleaseRequest.ToObjectReturnType0.IdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<ReleaseRequest.ToObjectReturnType0.IdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<ReleaseRequest.ToObjectReturnType0.IdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsOverlay
        static ReleaseRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        ReleaseRequest.ToObjectReturnType0.IdFieldType getId();

        @JsProperty
        void setId(ReleaseRequest.ToObjectReturnType0.IdFieldType id);
    }

    public static native ReleaseRequest deserializeBinary(Uint8Array bytes);

    public static native ReleaseRequest deserializeBinaryFromReader(
            ReleaseRequest message, Object reader);

    public static native void serializeBinaryToWriter(ReleaseRequest message, Object writer);

    public static native ReleaseRequest.ToObjectReturnType toObject(
            boolean includeInstance, ReleaseRequest msg);

    public native void clearId();

    public native Ticket getId();

    public native boolean hasId();

    public native Uint8Array serializeBinary();

    public native void setId();

    public native void setId(Ticket value);

    public native ReleaseRequest.ToObjectReturnType0 toObject();

    public native ReleaseRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
