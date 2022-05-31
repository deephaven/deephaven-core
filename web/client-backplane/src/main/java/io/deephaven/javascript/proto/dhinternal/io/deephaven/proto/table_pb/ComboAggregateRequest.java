package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.comboaggregaterequest.AggTypeMap;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.comboaggregaterequest.Aggregate;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.Ticket;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.ComboAggregateRequest",
        namespace = JsPackage.GLOBAL)
public class ComboAggregateRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface AggregatesListFieldType {
            @JsOverlay
            static ComboAggregateRequest.ToObjectReturnType.AggregatesListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getColumnName();

            @JsProperty
            JsArray<String> getMatchPairsList();

            @JsProperty
            double getPercentile();

            @JsProperty
            double getType();

            @JsProperty
            boolean isAvgMedian();

            @JsProperty
            void setAvgMedian(boolean avgMedian);

            @JsProperty
            void setColumnName(String columnName);

            @JsProperty
            void setMatchPairsList(JsArray<String> matchPairsList);

            @JsOverlay
            default void setMatchPairsList(String[] matchPairsList) {
                setMatchPairsList(Js.<JsArray<String>>uncheckedCast(matchPairsList));
            }

            @JsProperty
            void setPercentile(double percentile);

            @JsProperty
            void setType(double type);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static ComboAggregateRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType of(
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
            static ComboAggregateRequest.ToObjectReturnType.ResultIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            ComboAggregateRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    ComboAggregateRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<ComboAggregateRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<ComboAggregateRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SourceIdFieldType {
            @JsOverlay
            static ComboAggregateRequest.ToObjectReturnType.SourceIdFieldType create() {
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
        static ComboAggregateRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<ComboAggregateRequest.ToObjectReturnType.AggregatesListFieldType> getAggregatesList();

        @JsProperty
        JsArray<String> getGroupByColumnsList();

        @JsProperty
        ComboAggregateRequest.ToObjectReturnType.ResultIdFieldType getResultId();

        @JsProperty
        ComboAggregateRequest.ToObjectReturnType.SourceIdFieldType getSourceId();

        @JsProperty
        boolean isForceCombo();

        @JsOverlay
        default void setAggregatesList(
                ComboAggregateRequest.ToObjectReturnType.AggregatesListFieldType[] aggregatesList) {
            setAggregatesList(
                    Js.<JsArray<ComboAggregateRequest.ToObjectReturnType.AggregatesListFieldType>>uncheckedCast(
                            aggregatesList));
        }

        @JsProperty
        void setAggregatesList(
                JsArray<ComboAggregateRequest.ToObjectReturnType.AggregatesListFieldType> aggregatesList);

        @JsProperty
        void setForceCombo(boolean forceCombo);

        @JsProperty
        void setGroupByColumnsList(JsArray<String> groupByColumnsList);

        @JsOverlay
        default void setGroupByColumnsList(String[] groupByColumnsList) {
            setGroupByColumnsList(Js.<JsArray<String>>uncheckedCast(groupByColumnsList));
        }

        @JsProperty
        void setResultId(ComboAggregateRequest.ToObjectReturnType.ResultIdFieldType resultId);

        @JsProperty
        void setSourceId(ComboAggregateRequest.ToObjectReturnType.SourceIdFieldType sourceId);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface AggregatesListFieldType {
            @JsOverlay
            static ComboAggregateRequest.ToObjectReturnType0.AggregatesListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getColumnName();

            @JsProperty
            JsArray<String> getMatchPairsList();

            @JsProperty
            double getPercentile();

            @JsProperty
            double getType();

            @JsProperty
            boolean isAvgMedian();

            @JsProperty
            void setAvgMedian(boolean avgMedian);

            @JsProperty
            void setColumnName(String columnName);

            @JsProperty
            void setMatchPairsList(JsArray<String> matchPairsList);

            @JsOverlay
            default void setMatchPairsList(String[] matchPairsList) {
                setMatchPairsList(Js.<JsArray<String>>uncheckedCast(matchPairsList));
            }

            @JsProperty
            void setPercentile(double percentile);

            @JsProperty
            void setType(double type);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static ComboAggregateRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType of(
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
            static ComboAggregateRequest.ToObjectReturnType0.ResultIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            ComboAggregateRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    ComboAggregateRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<ComboAggregateRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<ComboAggregateRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SourceIdFieldType {
            @JsOverlay
            static ComboAggregateRequest.ToObjectReturnType0.SourceIdFieldType create() {
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
        static ComboAggregateRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<ComboAggregateRequest.ToObjectReturnType0.AggregatesListFieldType> getAggregatesList();

        @JsProperty
        JsArray<String> getGroupByColumnsList();

        @JsProperty
        ComboAggregateRequest.ToObjectReturnType0.ResultIdFieldType getResultId();

        @JsProperty
        ComboAggregateRequest.ToObjectReturnType0.SourceIdFieldType getSourceId();

        @JsProperty
        boolean isForceCombo();

        @JsOverlay
        default void setAggregatesList(
                ComboAggregateRequest.ToObjectReturnType0.AggregatesListFieldType[] aggregatesList) {
            setAggregatesList(
                    Js.<JsArray<ComboAggregateRequest.ToObjectReturnType0.AggregatesListFieldType>>uncheckedCast(
                            aggregatesList));
        }

        @JsProperty
        void setAggregatesList(
                JsArray<ComboAggregateRequest.ToObjectReturnType0.AggregatesListFieldType> aggregatesList);

        @JsProperty
        void setForceCombo(boolean forceCombo);

        @JsProperty
        void setGroupByColumnsList(JsArray<String> groupByColumnsList);

        @JsOverlay
        default void setGroupByColumnsList(String[] groupByColumnsList) {
            setGroupByColumnsList(Js.<JsArray<String>>uncheckedCast(groupByColumnsList));
        }

        @JsProperty
        void setResultId(ComboAggregateRequest.ToObjectReturnType0.ResultIdFieldType resultId);

        @JsProperty
        void setSourceId(ComboAggregateRequest.ToObjectReturnType0.SourceIdFieldType sourceId);
    }

    public static AggTypeMap AggType;

    public static native ComboAggregateRequest deserializeBinary(Uint8Array bytes);

    public static native ComboAggregateRequest deserializeBinaryFromReader(
            ComboAggregateRequest message, Object reader);

    public static native void serializeBinaryToWriter(ComboAggregateRequest message, Object writer);

    public static native ComboAggregateRequest.ToObjectReturnType toObject(
            boolean includeInstance, ComboAggregateRequest msg);

    public native Aggregate addAggregates();

    public native Aggregate addAggregates(Aggregate value, double index);

    public native Aggregate addAggregates(Aggregate value);

    public native String addGroupByColumns(String value, double index);

    public native String addGroupByColumns(String value);

    public native void clearAggregatesList();

    public native void clearGroupByColumnsList();

    public native void clearResultId();

    public native void clearSourceId();

    public native JsArray<Aggregate> getAggregatesList();

    public native boolean getForceCombo();

    public native JsArray<String> getGroupByColumnsList();

    public native Ticket getResultId();

    public native TableReference getSourceId();

    public native boolean hasResultId();

    public native boolean hasSourceId();

    public native Uint8Array serializeBinary();

    @JsOverlay
    public final void setAggregatesList(Aggregate[] value) {
        setAggregatesList(Js.<JsArray<Aggregate>>uncheckedCast(value));
    }

    public native void setAggregatesList(JsArray<Aggregate> value);

    public native void setForceCombo(boolean value);

    public native void setGroupByColumnsList(JsArray<String> value);

    @JsOverlay
    public final void setGroupByColumnsList(String[] value) {
        setGroupByColumnsList(Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void setResultId();

    public native void setResultId(Ticket value);

    public native void setSourceId();

    public native void setSourceId(TableReference value);

    public native ComboAggregateRequest.ToObjectReturnType0 toObject();

    public native ComboAggregateRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
