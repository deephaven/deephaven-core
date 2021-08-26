package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.ticket_pb.Ticket",
        namespace = JsPackage.GLOBAL)
public class Ticket {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetTicketUnionType {
        @JsOverlay
        static Ticket.GetTicketUnionType of(Object o) {
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
    public interface SetTicketValueUnionType {
        @JsOverlay
        static Ticket.SetTicketValueUnionType of(Object o) {
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
        public interface GetTicketUnionType {
            @JsOverlay
            static Ticket.ToObjectReturnType.GetTicketUnionType of(Object o) {
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
        static Ticket.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Ticket.ToObjectReturnType.GetTicketUnionType getTicket();

        @JsProperty
        void setTicket(Ticket.ToObjectReturnType.GetTicketUnionType ticket);

        @JsOverlay
        default void setTicket(String ticket) {
            setTicket(Js.<Ticket.ToObjectReturnType.GetTicketUnionType>uncheckedCast(ticket));
        }

        @JsOverlay
        default void setTicket(Uint8Array ticket) {
            setTicket(Js.<Ticket.ToObjectReturnType.GetTicketUnionType>uncheckedCast(ticket));
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetTicketUnionType {
            @JsOverlay
            static Ticket.ToObjectReturnType0.GetTicketUnionType of(Object o) {
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
        static Ticket.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Ticket.ToObjectReturnType0.GetTicketUnionType getTicket();

        @JsProperty
        void setTicket(Ticket.ToObjectReturnType0.GetTicketUnionType ticket);

        @JsOverlay
        default void setTicket(String ticket) {
            setTicket(Js.<Ticket.ToObjectReturnType0.GetTicketUnionType>uncheckedCast(ticket));
        }

        @JsOverlay
        default void setTicket(Uint8Array ticket) {
            setTicket(Js.<Ticket.ToObjectReturnType0.GetTicketUnionType>uncheckedCast(ticket));
        }
    }

    public static native Ticket deserializeBinary(Uint8Array bytes);

    public static native Ticket deserializeBinaryFromReader(Ticket message, Object reader);

    public static native void serializeBinaryToWriter(Ticket message, Object writer);

    public static native Ticket.ToObjectReturnType toObject(boolean includeInstance, Ticket msg);

    public native Ticket.GetTicketUnionType getTicket();

    public native String getTicket_asB64();

    public native Uint8Array getTicket_asU8();

    public native Uint8Array serializeBinary();

    public native void setTicket(Ticket.SetTicketValueUnionType value);

    @JsOverlay
    public final void setTicket(String value) {
        setTicket(Js.<Ticket.SetTicketValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public final void setTicket(Uint8Array value) {
        setTicket(Js.<Ticket.SetTicketValueUnionType>uncheckedCast(value));
    }

    public native Ticket.ToObjectReturnType0 toObject();

    public native Ticket.ToObjectReturnType0 toObject(boolean includeInstance);
}
