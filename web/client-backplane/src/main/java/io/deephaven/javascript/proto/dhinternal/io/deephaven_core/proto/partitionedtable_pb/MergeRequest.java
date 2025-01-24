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
        name = "dhinternal.io.deephaven_core.proto.partitionedtable_pb.MergeRequest",
        namespace = JsPackage.GLOBAL)
public class MergeRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface PartitionedTableFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static MergeRequest.ToObjectReturnType.PartitionedTableFieldType.GetTicketUnionType of(
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
            static MergeRequest.ToObjectReturnType.PartitionedTableFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            MergeRequest.ToObjectReturnType.PartitionedTableFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    MergeRequest.ToObjectReturnType.PartitionedTableFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<MergeRequest.ToObjectReturnType.PartitionedTableFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<MergeRequest.ToObjectReturnType.PartitionedTableFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsOverlay
        static MergeRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        MergeRequest.ToObjectReturnType.PartitionedTableFieldType getPartitionedTable();

        @JsProperty
        Object getResultId();

        @JsProperty
        void setPartitionedTable(
                MergeRequest.ToObjectReturnType.PartitionedTableFieldType partitionedTable);

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
                static MergeRequest.ToObjectReturnType0.PartitionedTableFieldType.GetTicketUnionType of(
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
            static MergeRequest.ToObjectReturnType0.PartitionedTableFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            MergeRequest.ToObjectReturnType0.PartitionedTableFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    MergeRequest.ToObjectReturnType0.PartitionedTableFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<MergeRequest.ToObjectReturnType0.PartitionedTableFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<MergeRequest.ToObjectReturnType0.PartitionedTableFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsOverlay
        static MergeRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        MergeRequest.ToObjectReturnType0.PartitionedTableFieldType getPartitionedTable();

        @JsProperty
        Object getResultId();

        @JsProperty
        void setPartitionedTable(
                MergeRequest.ToObjectReturnType0.PartitionedTableFieldType partitionedTable);

        @JsProperty
        void setResultId(Object resultId);
    }

    public static native MergeRequest deserializeBinary(Uint8Array bytes);

    public static native MergeRequest deserializeBinaryFromReader(
            MergeRequest message, Object reader);

    public static native void serializeBinaryToWriter(MergeRequest message, Object writer);

    public static native MergeRequest.ToObjectReturnType toObject(
            boolean includeInstance, MergeRequest msg);

    public native void clearPartitionedTable();

    public native void clearResultId();

    public native Ticket getPartitionedTable();

    public native Ticket getResultId();

    public native boolean hasPartitionedTable();

    public native boolean hasResultId();

    public native Uint8Array serializeBinary();

    public native void setPartitionedTable();

    public native void setPartitionedTable(Ticket value);

    public native void setResultId();

    public native void setResultId(Ticket value);

    public native MergeRequest.ToObjectReturnType0 toObject();

    public native MergeRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
