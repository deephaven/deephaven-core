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
        name = "dhinternal.io.deephaven.proto.ticket_pb.TypedTicket",
        namespace = JsPackage.GLOBAL)
public class TypedTicket {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetTicketUnionType {
        @JsOverlay
        static TypedTicket.GetTicketUnionType of(Object o) {
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
        static TypedTicket.SetTicketValueUnionType of(Object o) {
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
            static TypedTicket.ToObjectReturnType.GetTicketUnionType of(Object o) {
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
        static TypedTicket.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        TypedTicket.ToObjectReturnType.GetTicketUnionType getTicket();

        @JsProperty
        String getType();

        @JsProperty
        void setTicket(TypedTicket.ToObjectReturnType.GetTicketUnionType ticket);

        @JsOverlay
        default void setTicket(String ticket) {
            setTicket(Js.<TypedTicket.ToObjectReturnType.GetTicketUnionType>uncheckedCast(ticket));
        }

        @JsOverlay
        default void setTicket(Uint8Array ticket) {
            setTicket(Js.<TypedTicket.ToObjectReturnType.GetTicketUnionType>uncheckedCast(ticket));
        }

        @JsProperty
        void setType(String type);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetTicketUnionType {
            @JsOverlay
            static TypedTicket.ToObjectReturnType0.GetTicketUnionType of(Object o) {
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
        static TypedTicket.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        TypedTicket.ToObjectReturnType0.GetTicketUnionType getTicket();

        @JsProperty
        String getType();

        @JsProperty
        void setTicket(TypedTicket.ToObjectReturnType0.GetTicketUnionType ticket);

        @JsOverlay
        default void setTicket(String ticket) {
            setTicket(Js.<TypedTicket.ToObjectReturnType0.GetTicketUnionType>uncheckedCast(ticket));
        }

        @JsOverlay
        default void setTicket(Uint8Array ticket) {
            setTicket(Js.<TypedTicket.ToObjectReturnType0.GetTicketUnionType>uncheckedCast(ticket));
        }

        @JsProperty
        void setType(String type);
    }

    public static native TypedTicket deserializeBinary(Uint8Array bytes);

    public static native TypedTicket deserializeBinaryFromReader(TypedTicket message, Object reader);

    public static native void serializeBinaryToWriter(TypedTicket message, Object writer);

    public static native TypedTicket.ToObjectReturnType toObject(
            boolean includeInstance, TypedTicket msg);

    public native void clearType();

    public native TypedTicket.GetTicketUnionType getTicket();

    public native String getTicket_asB64();

    public native Uint8Array getTicket_asU8();

    public native String getType();

    public native boolean hasType();

    public native Uint8Array serializeBinary();

    public native void setTicket(TypedTicket.SetTicketValueUnionType value);

    @JsOverlay
    public final void setTicket(String value) {
        setTicket(Js.<TypedTicket.SetTicketValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public final void setTicket(Uint8Array value) {
        setTicket(Js.<TypedTicket.SetTicketValueUnionType>uncheckedCast(value));
    }

    public native void setType(String value);

    public native TypedTicket.ToObjectReturnType0 toObject();

    public native TypedTicket.ToObjectReturnType0 toObject(boolean includeInstance);
}
