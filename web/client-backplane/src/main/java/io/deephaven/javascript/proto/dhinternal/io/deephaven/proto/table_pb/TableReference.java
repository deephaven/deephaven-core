package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb;

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
    name = "dhinternal.io.deephaven.proto.table_pb.TableReference",
    namespace = JsPackage.GLOBAL)
public class TableReference {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface TicketFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static TableReference.ToObjectReturnType.TicketFieldType.GetTicketUnionType of(
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
            static TableReference.ToObjectReturnType.TicketFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            TableReference.ToObjectReturnType.TicketFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                TableReference.ToObjectReturnType.TicketFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                    Js.<TableReference.ToObjectReturnType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                        ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                    Js.<TableReference.ToObjectReturnType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                        ticket));
            }
        }

        @JsOverlay
        static TableReference.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getBatchOffset();

        @JsProperty
        TableReference.ToObjectReturnType.TicketFieldType getTicket();

        @JsProperty
        void setBatchOffset(double batchOffset);

        @JsProperty
        void setTicket(TableReference.ToObjectReturnType.TicketFieldType ticket);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface TicketFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static TableReference.ToObjectReturnType0.TicketFieldType.GetTicketUnionType of(
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
            static TableReference.ToObjectReturnType0.TicketFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            TableReference.ToObjectReturnType0.TicketFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                TableReference.ToObjectReturnType0.TicketFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                    Js.<TableReference.ToObjectReturnType0.TicketFieldType.GetTicketUnionType>uncheckedCast(
                        ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                    Js.<TableReference.ToObjectReturnType0.TicketFieldType.GetTicketUnionType>uncheckedCast(
                        ticket));
            }
        }

        @JsOverlay
        static TableReference.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getBatchOffset();

        @JsProperty
        TableReference.ToObjectReturnType0.TicketFieldType getTicket();

        @JsProperty
        void setBatchOffset(double batchOffset);

        @JsProperty
        void setTicket(TableReference.ToObjectReturnType0.TicketFieldType ticket);
    }

    public static native TableReference deserializeBinary(Uint8Array bytes);

    public static native TableReference deserializeBinaryFromReader(
        TableReference message, Object reader);

    public static native void serializeBinaryToWriter(TableReference message, Object writer);

    public static native TableReference.ToObjectReturnType toObject(
        boolean includeInstance, TableReference msg);

    public native void clearBatchOffset();

    public native void clearTicket();

    public native int getBatchOffset();

    public native int getRefCase();

    public native Ticket getTicket();

    public native boolean hasBatchOffset();

    public native boolean hasTicket();

    public native Uint8Array serializeBinary();

    public native void setBatchOffset(int value);

    public native void setTicket();

    public native void setTicket(Ticket value);

    public native TableReference.ToObjectReturnType0 toObject();

    public native TableReference.ToObjectReturnType0 toObject(boolean includeInstance);
}
