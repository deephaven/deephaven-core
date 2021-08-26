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
    name = "dhinternal.io.deephaven.proto.console_pb.FetchTableMapRequest",
    namespace = JsPackage.GLOBAL)
public class FetchTableMapRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ConsoleIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static FetchTableMapRequest.ToObjectReturnType.ConsoleIdFieldType.GetTicketUnionType of(
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
            static FetchTableMapRequest.ToObjectReturnType.ConsoleIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            FetchTableMapRequest.ToObjectReturnType.ConsoleIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                FetchTableMapRequest.ToObjectReturnType.ConsoleIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                    Js.<FetchTableMapRequest.ToObjectReturnType.ConsoleIdFieldType.GetTicketUnionType>uncheckedCast(
                        ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                    Js.<FetchTableMapRequest.ToObjectReturnType.ConsoleIdFieldType.GetTicketUnionType>uncheckedCast(
                        ticket));
            }
        }

        @JsOverlay
        static FetchTableMapRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        FetchTableMapRequest.ToObjectReturnType.ConsoleIdFieldType getConsoleId();

        @JsProperty
        void setConsoleId(FetchTableMapRequest.ToObjectReturnType.ConsoleIdFieldType consoleId);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ConsoleIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static FetchTableMapRequest.ToObjectReturnType0.ConsoleIdFieldType.GetTicketUnionType of(
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
            static FetchTableMapRequest.ToObjectReturnType0.ConsoleIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            FetchTableMapRequest.ToObjectReturnType0.ConsoleIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                FetchTableMapRequest.ToObjectReturnType0.ConsoleIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                    Js.<FetchTableMapRequest.ToObjectReturnType0.ConsoleIdFieldType.GetTicketUnionType>uncheckedCast(
                        ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                    Js.<FetchTableMapRequest.ToObjectReturnType0.ConsoleIdFieldType.GetTicketUnionType>uncheckedCast(
                        ticket));
            }
        }

        @JsOverlay
        static FetchTableMapRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        FetchTableMapRequest.ToObjectReturnType0.ConsoleIdFieldType getConsoleId();

        @JsProperty
        void setConsoleId(FetchTableMapRequest.ToObjectReturnType0.ConsoleIdFieldType consoleId);
    }

    public static native FetchTableMapRequest deserializeBinary(Uint8Array bytes);

    public static native FetchTableMapRequest deserializeBinaryFromReader(
        FetchTableMapRequest message, Object reader);

    public static native void serializeBinaryToWriter(FetchTableMapRequest message, Object writer);

    public static native FetchTableMapRequest.ToObjectReturnType toObject(
        boolean includeInstance, FetchTableMapRequest msg);

    public native void clearConsoleId();

    public native Ticket getConsoleId();

    public native boolean hasConsoleId();

    public native Uint8Array serializeBinary();

    public native void setConsoleId();

    public native void setConsoleId(Ticket value);

    public native FetchTableMapRequest.ToObjectReturnType0 toObject();

    public native FetchTableMapRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
