package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb;

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
    name = "dhinternal.io.deephaven.proto.console_pb.CancelCommandRequest",
    namespace = JsPackage.GLOBAL)
public class CancelCommandRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ConsoleIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static CancelCommandRequest.ToObjectReturnType.ConsoleIdFieldType.GetTicketUnionType of(
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
            static CancelCommandRequest.ToObjectReturnType.ConsoleIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            CancelCommandRequest.ToObjectReturnType.ConsoleIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                CancelCommandRequest.ToObjectReturnType.ConsoleIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                    Js.<CancelCommandRequest.ToObjectReturnType.ConsoleIdFieldType.GetTicketUnionType>uncheckedCast(
                        ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                    Js.<CancelCommandRequest.ToObjectReturnType.ConsoleIdFieldType.GetTicketUnionType>uncheckedCast(
                        ticket));
            }
        }

        @JsOverlay
        static CancelCommandRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Object getCommandId();

        @JsProperty
        CancelCommandRequest.ToObjectReturnType.ConsoleIdFieldType getConsoleId();

        @JsProperty
        void setCommandId(Object commandId);

        @JsProperty
        void setConsoleId(CancelCommandRequest.ToObjectReturnType.ConsoleIdFieldType consoleId);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ConsoleIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static CancelCommandRequest.ToObjectReturnType0.ConsoleIdFieldType.GetTicketUnionType of(
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
            static CancelCommandRequest.ToObjectReturnType0.ConsoleIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            CancelCommandRequest.ToObjectReturnType0.ConsoleIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                CancelCommandRequest.ToObjectReturnType0.ConsoleIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                    Js.<CancelCommandRequest.ToObjectReturnType0.ConsoleIdFieldType.GetTicketUnionType>uncheckedCast(
                        ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                    Js.<CancelCommandRequest.ToObjectReturnType0.ConsoleIdFieldType.GetTicketUnionType>uncheckedCast(
                        ticket));
            }
        }

        @JsOverlay
        static CancelCommandRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Object getCommandId();

        @JsProperty
        CancelCommandRequest.ToObjectReturnType0.ConsoleIdFieldType getConsoleId();

        @JsProperty
        void setCommandId(Object commandId);

        @JsProperty
        void setConsoleId(CancelCommandRequest.ToObjectReturnType0.ConsoleIdFieldType consoleId);
    }

    public static native CancelCommandRequest deserializeBinary(Uint8Array bytes);

    public static native CancelCommandRequest deserializeBinaryFromReader(
        CancelCommandRequest message, Object reader);

    public static native void serializeBinaryToWriter(CancelCommandRequest message, Object writer);

    public static native CancelCommandRequest.ToObjectReturnType toObject(
        boolean includeInstance, CancelCommandRequest msg);

    public native void clearCommandId();

    public native void clearConsoleId();

    public native Ticket getCommandId();

    public native Ticket getConsoleId();

    public native boolean hasCommandId();

    public native boolean hasConsoleId();

    public native Uint8Array serializeBinary();

    public native void setCommandId();

    public native void setCommandId(Ticket value);

    public native void setConsoleId();

    public native void setConsoleId(Ticket value);

    public native CancelCommandRequest.ToObjectReturnType0 toObject();

    public native CancelCommandRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
