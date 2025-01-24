//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.hierarchicaltable_pb;

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
        name = "dhinternal.io.deephaven_core.proto.hierarchicaltable_pb.HierarchicalTableSourceExportRequest",
        namespace = JsPackage.GLOBAL)
public class HierarchicalTableSourceExportRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultTableIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static HierarchicalTableSourceExportRequest.ToObjectReturnType.ResultTableIdFieldType.GetTicketUnionType of(
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
            static HierarchicalTableSourceExportRequest.ToObjectReturnType.ResultTableIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            HierarchicalTableSourceExportRequest.ToObjectReturnType.ResultTableIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    HierarchicalTableSourceExportRequest.ToObjectReturnType.ResultTableIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<HierarchicalTableSourceExportRequest.ToObjectReturnType.ResultTableIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<HierarchicalTableSourceExportRequest.ToObjectReturnType.ResultTableIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsOverlay
        static HierarchicalTableSourceExportRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Object getHierarchicalTableId();

        @JsProperty
        HierarchicalTableSourceExportRequest.ToObjectReturnType.ResultTableIdFieldType getResultTableId();

        @JsProperty
        void setHierarchicalTableId(Object hierarchicalTableId);

        @JsProperty
        void setResultTableId(
                HierarchicalTableSourceExportRequest.ToObjectReturnType.ResultTableIdFieldType resultTableId);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultTableIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static HierarchicalTableSourceExportRequest.ToObjectReturnType0.ResultTableIdFieldType.GetTicketUnionType of(
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
            static HierarchicalTableSourceExportRequest.ToObjectReturnType0.ResultTableIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            HierarchicalTableSourceExportRequest.ToObjectReturnType0.ResultTableIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    HierarchicalTableSourceExportRequest.ToObjectReturnType0.ResultTableIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<HierarchicalTableSourceExportRequest.ToObjectReturnType0.ResultTableIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<HierarchicalTableSourceExportRequest.ToObjectReturnType0.ResultTableIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsOverlay
        static HierarchicalTableSourceExportRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Object getHierarchicalTableId();

        @JsProperty
        HierarchicalTableSourceExportRequest.ToObjectReturnType0.ResultTableIdFieldType getResultTableId();

        @JsProperty
        void setHierarchicalTableId(Object hierarchicalTableId);

        @JsProperty
        void setResultTableId(
                HierarchicalTableSourceExportRequest.ToObjectReturnType0.ResultTableIdFieldType resultTableId);
    }

    public static native HierarchicalTableSourceExportRequest deserializeBinary(Uint8Array bytes);

    public static native HierarchicalTableSourceExportRequest deserializeBinaryFromReader(
            HierarchicalTableSourceExportRequest message, Object reader);

    public static native void serializeBinaryToWriter(
            HierarchicalTableSourceExportRequest message, Object writer);

    public static native HierarchicalTableSourceExportRequest.ToObjectReturnType toObject(
            boolean includeInstance, HierarchicalTableSourceExportRequest msg);

    public native void clearHierarchicalTableId();

    public native void clearResultTableId();

    public native Ticket getHierarchicalTableId();

    public native Ticket getResultTableId();

    public native boolean hasHierarchicalTableId();

    public native boolean hasResultTableId();

    public native Uint8Array serializeBinary();

    public native void setHierarchicalTableId();

    public native void setHierarchicalTableId(Ticket value);

    public native void setResultTableId();

    public native void setResultTableId(Ticket value);

    public native HierarchicalTableSourceExportRequest.ToObjectReturnType0 toObject();

    public native HierarchicalTableSourceExportRequest.ToObjectReturnType0 toObject(
            boolean includeInstance);
}
