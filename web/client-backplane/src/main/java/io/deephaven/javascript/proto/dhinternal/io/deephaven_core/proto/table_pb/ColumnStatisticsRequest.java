//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb;

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
        name = "dhinternal.io.deephaven_core.proto.table_pb.ColumnStatisticsRequest",
        namespace = JsPackage.GLOBAL)
public class ColumnStatisticsRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static ColumnStatisticsRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType of(
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
            static ColumnStatisticsRequest.ToObjectReturnType.ResultIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            ColumnStatisticsRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    ColumnStatisticsRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<ColumnStatisticsRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<ColumnStatisticsRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SourceIdFieldType {
            @JsOverlay
            static ColumnStatisticsRequest.ToObjectReturnType.SourceIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getBatchOffset();

            @JsProperty
            Object getTicket();

            @JsProperty
            void setBatchOffset(double batchOffset);

            @JsProperty
            void setTicket(Object ticket);
        }

        @JsOverlay
        static ColumnStatisticsRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getColumnName();

        @JsProperty
        ColumnStatisticsRequest.ToObjectReturnType.ResultIdFieldType getResultId();

        @JsProperty
        ColumnStatisticsRequest.ToObjectReturnType.SourceIdFieldType getSourceId();

        @JsProperty
        double getUniqueValueLimit();

        @JsProperty
        void setColumnName(String columnName);

        @JsProperty
        void setResultId(ColumnStatisticsRequest.ToObjectReturnType.ResultIdFieldType resultId);

        @JsProperty
        void setSourceId(ColumnStatisticsRequest.ToObjectReturnType.SourceIdFieldType sourceId);

        @JsProperty
        void setUniqueValueLimit(double uniqueValueLimit);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static ColumnStatisticsRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType of(
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
            static ColumnStatisticsRequest.ToObjectReturnType0.ResultIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            ColumnStatisticsRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    ColumnStatisticsRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<ColumnStatisticsRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<ColumnStatisticsRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SourceIdFieldType {
            @JsOverlay
            static ColumnStatisticsRequest.ToObjectReturnType0.SourceIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getBatchOffset();

            @JsProperty
            Object getTicket();

            @JsProperty
            void setBatchOffset(double batchOffset);

            @JsProperty
            void setTicket(Object ticket);
        }

        @JsOverlay
        static ColumnStatisticsRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getColumnName();

        @JsProperty
        ColumnStatisticsRequest.ToObjectReturnType0.ResultIdFieldType getResultId();

        @JsProperty
        ColumnStatisticsRequest.ToObjectReturnType0.SourceIdFieldType getSourceId();

        @JsProperty
        double getUniqueValueLimit();

        @JsProperty
        void setColumnName(String columnName);

        @JsProperty
        void setResultId(ColumnStatisticsRequest.ToObjectReturnType0.ResultIdFieldType resultId);

        @JsProperty
        void setSourceId(ColumnStatisticsRequest.ToObjectReturnType0.SourceIdFieldType sourceId);

        @JsProperty
        void setUniqueValueLimit(double uniqueValueLimit);
    }

    public static native ColumnStatisticsRequest deserializeBinary(Uint8Array bytes);

    public static native ColumnStatisticsRequest deserializeBinaryFromReader(
            ColumnStatisticsRequest message, Object reader);

    public static native void serializeBinaryToWriter(ColumnStatisticsRequest message, Object writer);

    public static native ColumnStatisticsRequest.ToObjectReturnType toObject(
            boolean includeInstance, ColumnStatisticsRequest msg);

    public native void clearResultId();

    public native void clearSourceId();

    public native void clearUniqueValueLimit();

    public native String getColumnName();

    public native Ticket getResultId();

    public native TableReference getSourceId();

    public native int getUniqueValueLimit();

    public native boolean hasResultId();

    public native boolean hasSourceId();

    public native boolean hasUniqueValueLimit();

    public native Uint8Array serializeBinary();

    public native void setColumnName(String value);

    public native void setResultId();

    public native void setResultId(Ticket value);

    public native void setSourceId();

    public native void setSourceId(TableReference value);

    public native void setUniqueValueLimit(int value);

    public native ColumnStatisticsRequest.ToObjectReturnType0 toObject();

    public native ColumnStatisticsRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
