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
    name = "dhinternal.io.deephaven.proto.console_pb.FetchPandasTableRequest",
    namespace = JsPackage.GLOBAL)
public class FetchPandasTableRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ConsoleIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static FetchPandasTableRequest.ToObjectReturnType.ConsoleIdFieldType.GetTicketUnionType of(
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
            static FetchPandasTableRequest.ToObjectReturnType.ConsoleIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            FetchPandasTableRequest.ToObjectReturnType.ConsoleIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                FetchPandasTableRequest.ToObjectReturnType.ConsoleIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                    Js.<FetchPandasTableRequest.ToObjectReturnType.ConsoleIdFieldType.GetTicketUnionType>uncheckedCast(
                        ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                    Js.<FetchPandasTableRequest.ToObjectReturnType.ConsoleIdFieldType.GetTicketUnionType>uncheckedCast(
                        ticket));
            }
        }

        @JsOverlay
        static FetchPandasTableRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        FetchPandasTableRequest.ToObjectReturnType.ConsoleIdFieldType getConsoleId();

        @JsProperty
        Object getTableId();

        @JsProperty
        void setConsoleId(FetchPandasTableRequest.ToObjectReturnType.ConsoleIdFieldType consoleId);

        @JsProperty
        void setTableId(Object tableId);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ConsoleIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static FetchPandasTableRequest.ToObjectReturnType0.ConsoleIdFieldType.GetTicketUnionType of(
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
            static FetchPandasTableRequest.ToObjectReturnType0.ConsoleIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            FetchPandasTableRequest.ToObjectReturnType0.ConsoleIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                FetchPandasTableRequest.ToObjectReturnType0.ConsoleIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                    Js.<FetchPandasTableRequest.ToObjectReturnType0.ConsoleIdFieldType.GetTicketUnionType>uncheckedCast(
                        ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                    Js.<FetchPandasTableRequest.ToObjectReturnType0.ConsoleIdFieldType.GetTicketUnionType>uncheckedCast(
                        ticket));
            }
        }

        @JsOverlay
        static FetchPandasTableRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        FetchPandasTableRequest.ToObjectReturnType0.ConsoleIdFieldType getConsoleId();

        @JsProperty
        Object getTableId();

        @JsProperty
        void setConsoleId(FetchPandasTableRequest.ToObjectReturnType0.ConsoleIdFieldType consoleId);

        @JsProperty
        void setTableId(Object tableId);
    }

    public static native FetchPandasTableRequest deserializeBinary(Uint8Array bytes);

    public static native FetchPandasTableRequest deserializeBinaryFromReader(
        FetchPandasTableRequest message, Object reader);

    public static native void serializeBinaryToWriter(FetchPandasTableRequest message,
        Object writer);

    public static native FetchPandasTableRequest.ToObjectReturnType toObject(
        boolean includeInstance, FetchPandasTableRequest msg);

    public native void clearConsoleId();

    public native void clearTableId();

    public native Ticket getConsoleId();

    public native Ticket getTableId();

    public native boolean hasConsoleId();

    public native boolean hasTableId();

    public native Uint8Array serializeBinary();

    public native void setConsoleId();

    public native void setConsoleId(Ticket value);

    public native void setTableId();

    public native void setTableId(Ticket value);

    public native FetchPandasTableRequest.ToObjectReturnType0 toObject();

    public native FetchPandasTableRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
