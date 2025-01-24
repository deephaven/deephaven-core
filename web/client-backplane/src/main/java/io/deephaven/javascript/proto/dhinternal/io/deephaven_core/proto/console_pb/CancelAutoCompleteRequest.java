//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb;

import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.ticket_pb.Ticket;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.console_pb.CancelAutoCompleteRequest",
        namespace = JsPackage.GLOBAL)
public class CancelAutoCompleteRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ConsoleIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static CancelAutoCompleteRequest.ToObjectReturnType.ConsoleIdFieldType.GetTicketUnionType of(Object o) {
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
            static CancelAutoCompleteRequest.ToObjectReturnType.ConsoleIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            CancelAutoCompleteRequest.ToObjectReturnType.ConsoleIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    CancelAutoCompleteRequest.ToObjectReturnType.ConsoleIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<CancelAutoCompleteRequest.ToObjectReturnType.ConsoleIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<CancelAutoCompleteRequest.ToObjectReturnType.ConsoleIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsOverlay
        static CancelAutoCompleteRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        CancelAutoCompleteRequest.ToObjectReturnType.ConsoleIdFieldType getConsoleId();

        @JsProperty
        double getRequestId();

        @JsProperty
        void setConsoleId(CancelAutoCompleteRequest.ToObjectReturnType.ConsoleIdFieldType consoleId);

        @JsProperty
        void setRequestId(double requestId);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ConsoleIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static CancelAutoCompleteRequest.ToObjectReturnType0.ConsoleIdFieldType.GetTicketUnionType of(
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
            static CancelAutoCompleteRequest.ToObjectReturnType0.ConsoleIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            CancelAutoCompleteRequest.ToObjectReturnType0.ConsoleIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    CancelAutoCompleteRequest.ToObjectReturnType0.ConsoleIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<CancelAutoCompleteRequest.ToObjectReturnType0.ConsoleIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<CancelAutoCompleteRequest.ToObjectReturnType0.ConsoleIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsOverlay
        static CancelAutoCompleteRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        CancelAutoCompleteRequest.ToObjectReturnType0.ConsoleIdFieldType getConsoleId();

        @JsProperty
        double getRequestId();

        @JsProperty
        void setConsoleId(CancelAutoCompleteRequest.ToObjectReturnType0.ConsoleIdFieldType consoleId);

        @JsProperty
        void setRequestId(double requestId);
    }

    public static native CancelAutoCompleteRequest deserializeBinary(Uint8Array bytes);

    public static native CancelAutoCompleteRequest deserializeBinaryFromReader(
            CancelAutoCompleteRequest message, Object reader);

    public static native void serializeBinaryToWriter(
            CancelAutoCompleteRequest message, Object writer);

    public static native CancelAutoCompleteRequest.ToObjectReturnType toObject(
            boolean includeInstance, CancelAutoCompleteRequest msg);

    public native void clearConsoleId();

    public native Ticket getConsoleId();

    public native double getRequestId();

    public native boolean hasConsoleId();

    public native Uint8Array serializeBinary();

    public native void setConsoleId();

    public native void setConsoleId(Ticket value);

    public native void setRequestId(double value);

    public native CancelAutoCompleteRequest.ToObjectReturnType0 toObject();

    public native CancelAutoCompleteRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
