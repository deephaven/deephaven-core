//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.partitionedtable_pb;

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
        name = "dhinternal.io.deephaven_core.proto.partitionedtable_pb.GetTableRequest",
        namespace = JsPackage.GLOBAL)
public class GetTableRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface PartitionedTableFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static GetTableRequest.ToObjectReturnType.PartitionedTableFieldType.GetTicketUnionType of(
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
            static GetTableRequest.ToObjectReturnType.PartitionedTableFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            GetTableRequest.ToObjectReturnType.PartitionedTableFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    GetTableRequest.ToObjectReturnType.PartitionedTableFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<GetTableRequest.ToObjectReturnType.PartitionedTableFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<GetTableRequest.ToObjectReturnType.PartitionedTableFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsOverlay
        static GetTableRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Object getKeyTableTicket();

        @JsProperty
        GetTableRequest.ToObjectReturnType.PartitionedTableFieldType getPartitionedTable();

        @JsProperty
        Object getResultId();

        @JsProperty
        void setKeyTableTicket(Object keyTableTicket);

        @JsProperty
        void setPartitionedTable(
                GetTableRequest.ToObjectReturnType.PartitionedTableFieldType partitionedTable);

        @JsProperty
        void setResultId(Object resultId);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface PartitionedTableFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static GetTableRequest.ToObjectReturnType0.PartitionedTableFieldType.GetTicketUnionType of(
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
            static GetTableRequest.ToObjectReturnType0.PartitionedTableFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            GetTableRequest.ToObjectReturnType0.PartitionedTableFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    GetTableRequest.ToObjectReturnType0.PartitionedTableFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<GetTableRequest.ToObjectReturnType0.PartitionedTableFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<GetTableRequest.ToObjectReturnType0.PartitionedTableFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsOverlay
        static GetTableRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Object getKeyTableTicket();

        @JsProperty
        GetTableRequest.ToObjectReturnType0.PartitionedTableFieldType getPartitionedTable();

        @JsProperty
        Object getResultId();

        @JsProperty
        void setKeyTableTicket(Object keyTableTicket);

        @JsProperty
        void setPartitionedTable(
                GetTableRequest.ToObjectReturnType0.PartitionedTableFieldType partitionedTable);

        @JsProperty
        void setResultId(Object resultId);
    }

    public static native GetTableRequest deserializeBinary(Uint8Array bytes);

    public static native GetTableRequest deserializeBinaryFromReader(
            GetTableRequest message, Object reader);

    public static native void serializeBinaryToWriter(GetTableRequest message, Object writer);

    public static native GetTableRequest.ToObjectReturnType toObject(
            boolean includeInstance, GetTableRequest msg);

    public native void clearKeyTableTicket();

    public native void clearPartitionedTable();

    public native void clearResultId();

    public native Ticket getKeyTableTicket();

    public native Ticket getPartitionedTable();

    public native Ticket getResultId();

    public native boolean hasKeyTableTicket();

    public native boolean hasPartitionedTable();

    public native boolean hasResultId();

    public native Uint8Array serializeBinary();

    public native void setKeyTableTicket();

    public native void setKeyTableTicket(Ticket value);

    public native void setPartitionedTable();

    public native void setPartitionedTable(Ticket value);

    public native void setResultId();

    public native void setResultId(Ticket value);

    public native GetTableRequest.ToObjectReturnType0 toObject();

    public native GetTableRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
