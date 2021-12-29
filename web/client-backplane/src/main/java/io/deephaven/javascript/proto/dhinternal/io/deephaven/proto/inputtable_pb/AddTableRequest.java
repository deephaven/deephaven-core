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
        name = "dhinternal.io.deephaven.proto.inputtable_pb.AddTableRequest",
        namespace = JsPackage.GLOBAL)
public class AddTableRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface InputTableFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static AddTableRequest.ToObjectReturnType.InputTableFieldType.GetTicketUnionType of(
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
            static AddTableRequest.ToObjectReturnType.InputTableFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            AddTableRequest.ToObjectReturnType.InputTableFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    AddTableRequest.ToObjectReturnType.InputTableFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<AddTableRequest.ToObjectReturnType.InputTableFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<AddTableRequest.ToObjectReturnType.InputTableFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsOverlay
        static AddTableRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        AddTableRequest.ToObjectReturnType.InputTableFieldType getInputTable();

        @JsProperty
        Object getTableToAdd();

        @JsProperty
        void setInputTable(AddTableRequest.ToObjectReturnType.InputTableFieldType inputTable);

        @JsProperty
        void setTableToAdd(Object tableToAdd);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface InputTableFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static AddTableRequest.ToObjectReturnType0.InputTableFieldType.GetTicketUnionType of(
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
            static AddTableRequest.ToObjectReturnType0.InputTableFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            AddTableRequest.ToObjectReturnType0.InputTableFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    AddTableRequest.ToObjectReturnType0.InputTableFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<AddTableRequest.ToObjectReturnType0.InputTableFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<AddTableRequest.ToObjectReturnType0.InputTableFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsOverlay
        static AddTableRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        AddTableRequest.ToObjectReturnType0.InputTableFieldType getInputTable();

        @JsProperty
        Object getTableToAdd();

        @JsProperty
        void setInputTable(AddTableRequest.ToObjectReturnType0.InputTableFieldType inputTable);

        @JsProperty
        void setTableToAdd(Object tableToAdd);
    }

    public static native AddTableRequest deserializeBinary(Uint8Array bytes);

    public static native AddTableRequest deserializeBinaryFromReader(
            AddTableRequest message, Object reader);

    public static native void serializeBinaryToWriter(AddTableRequest message, Object writer);

    public static native AddTableRequest.ToObjectReturnType toObject(
            boolean includeInstance, AddTableRequest msg);

    public native void clearInputTable();

    public native void clearTableToAdd();

    public native Ticket getInputTable();

    public native Ticket getTableToAdd();

    public native boolean hasInputTable();

    public native boolean hasTableToAdd();

    public native Uint8Array serializeBinary();

    public native void setInputTable();

    public native void setInputTable(Ticket value);

    public native void setTableToAdd();

    public native void setTableToAdd(Ticket value);

    public native AddTableRequest.ToObjectReturnType0 toObject();

    public native AddTableRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
