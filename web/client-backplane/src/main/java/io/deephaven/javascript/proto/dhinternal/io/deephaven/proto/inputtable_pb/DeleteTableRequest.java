package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.inputtable_pb;

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
        name = "dhinternal.io.deephaven.proto.inputtable_pb.DeleteTableRequest",
        namespace = JsPackage.GLOBAL)
public class DeleteTableRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface InputTableFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static DeleteTableRequest.ToObjectReturnType.InputTableFieldType.GetTicketUnionType of(
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
            static DeleteTableRequest.ToObjectReturnType.InputTableFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            DeleteTableRequest.ToObjectReturnType.InputTableFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    DeleteTableRequest.ToObjectReturnType.InputTableFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<DeleteTableRequest.ToObjectReturnType.InputTableFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<DeleteTableRequest.ToObjectReturnType.InputTableFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsOverlay
        static DeleteTableRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        DeleteTableRequest.ToObjectReturnType.InputTableFieldType getInputTable();

        @JsProperty
        Object getTableToRemove();

        @JsProperty
        void setInputTable(DeleteTableRequest.ToObjectReturnType.InputTableFieldType inputTable);

        @JsProperty
        void setTableToRemove(Object tableToRemove);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface InputTableFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static DeleteTableRequest.ToObjectReturnType0.InputTableFieldType.GetTicketUnionType of(
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
            static DeleteTableRequest.ToObjectReturnType0.InputTableFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            DeleteTableRequest.ToObjectReturnType0.InputTableFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    DeleteTableRequest.ToObjectReturnType0.InputTableFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<DeleteTableRequest.ToObjectReturnType0.InputTableFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<DeleteTableRequest.ToObjectReturnType0.InputTableFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsOverlay
        static DeleteTableRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        DeleteTableRequest.ToObjectReturnType0.InputTableFieldType getInputTable();

        @JsProperty
        Object getTableToRemove();

        @JsProperty
        void setInputTable(DeleteTableRequest.ToObjectReturnType0.InputTableFieldType inputTable);

        @JsProperty
        void setTableToRemove(Object tableToRemove);
    }

    public static native DeleteTableRequest deserializeBinary(Uint8Array bytes);

    public static native DeleteTableRequest deserializeBinaryFromReader(
            DeleteTableRequest message, Object reader);

    public static native void serializeBinaryToWriter(DeleteTableRequest message, Object writer);

    public static native DeleteTableRequest.ToObjectReturnType toObject(
            boolean includeInstance, DeleteTableRequest msg);

    public native void clearInputTable();

    public native void clearTableToRemove();

    public native Ticket getInputTable();

    public native Ticket getTableToRemove();

    public native boolean hasInputTable();

    public native boolean hasTableToRemove();

    public native Uint8Array serializeBinary();

    public native void setInputTable();

    public native void setInputTable(Ticket value);

    public native void setTableToRemove();

    public native void setTableToRemove(Ticket value);

    public native DeleteTableRequest.ToObjectReturnType0 toObject();

    public native DeleteTableRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
