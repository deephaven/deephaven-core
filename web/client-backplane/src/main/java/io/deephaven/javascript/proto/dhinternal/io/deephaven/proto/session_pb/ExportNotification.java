package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb;

import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.Ticket;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb.exportnotification.StateMap;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.session_pb.ExportNotification",
    namespace = JsPackage.GLOBAL)
public class ExportNotification {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface TicketFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static ExportNotification.ToObjectReturnType.TicketFieldType.GetTicketUnionType of(
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
            static ExportNotification.ToObjectReturnType.TicketFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            ExportNotification.ToObjectReturnType.TicketFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                ExportNotification.ToObjectReturnType.TicketFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                    Js.<ExportNotification.ToObjectReturnType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                        ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                    Js.<ExportNotification.ToObjectReturnType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                        ticket));
            }
        }

        @JsOverlay
        static ExportNotification.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getContext();

        @JsProperty
        String getDependentHandle();

        @JsProperty
        double getExportState();

        @JsProperty
        ExportNotification.ToObjectReturnType.TicketFieldType getTicket();

        @JsProperty
        void setContext(String context);

        @JsProperty
        void setDependentHandle(String dependentHandle);

        @JsProperty
        void setExportState(double exportState);

        @JsProperty
        void setTicket(ExportNotification.ToObjectReturnType.TicketFieldType ticket);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface TicketFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static ExportNotification.ToObjectReturnType0.TicketFieldType.GetTicketUnionType of(
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
            static ExportNotification.ToObjectReturnType0.TicketFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            ExportNotification.ToObjectReturnType0.TicketFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                ExportNotification.ToObjectReturnType0.TicketFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                    Js.<ExportNotification.ToObjectReturnType0.TicketFieldType.GetTicketUnionType>uncheckedCast(
                        ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                    Js.<ExportNotification.ToObjectReturnType0.TicketFieldType.GetTicketUnionType>uncheckedCast(
                        ticket));
            }
        }

        @JsOverlay
        static ExportNotification.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getContext();

        @JsProperty
        String getDependentHandle();

        @JsProperty
        double getExportState();

        @JsProperty
        ExportNotification.ToObjectReturnType0.TicketFieldType getTicket();

        @JsProperty
        void setContext(String context);

        @JsProperty
        void setDependentHandle(String dependentHandle);

        @JsProperty
        void setExportState(double exportState);

        @JsProperty
        void setTicket(ExportNotification.ToObjectReturnType0.TicketFieldType ticket);
    }

    public static StateMap State;

    public static native ExportNotification deserializeBinary(Uint8Array bytes);

    public static native ExportNotification deserializeBinaryFromReader(
        ExportNotification message, Object reader);

    public static native void serializeBinaryToWriter(ExportNotification message, Object writer);

    public static native ExportNotification.ToObjectReturnType toObject(
        boolean includeInstance, ExportNotification msg);

    public native void clearTicket();

    public native String getContext();

    public native String getDependentHandle();

    public native double getExportState();

    public native Ticket getTicket();

    public native boolean hasTicket();

    public native Uint8Array serializeBinary();

    public native void setContext(String value);

    public native void setDependentHandle(String value);

    public native void setExportState(double value);

    public native void setTicket();

    public native void setTicket(Ticket value);

    public native ExportNotification.ToObjectReturnType0 toObject();

    public native ExportNotification.ToObjectReturnType0 toObject(boolean includeInstance);
}
