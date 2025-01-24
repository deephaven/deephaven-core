//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb;

import elemental2.core.JsArray;
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
        name = "dhinternal.io.deephaven_core.proto.table_pb.AggregateAllRequest",
        namespace = JsPackage.GLOBAL)
public class AggregateAllRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static AggregateAllRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType of(
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
            static AggregateAllRequest.ToObjectReturnType.ResultIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            AggregateAllRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    AggregateAllRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<AggregateAllRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<AggregateAllRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SourceIdFieldType {
            @JsOverlay
            static AggregateAllRequest.ToObjectReturnType.SourceIdFieldType create() {
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

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SpecFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ApproximatePercentileFieldType {
                @JsOverlay
                static AggregateAllRequest.ToObjectReturnType.SpecFieldType.ApproximatePercentileFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                double getCompression();

                @JsProperty
                double getPercentile();

                @JsProperty
                void setCompression(double compression);

                @JsProperty
                void setPercentile(double percentile);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface CountDistinctFieldType {
                @JsOverlay
                static AggregateAllRequest.ToObjectReturnType.SpecFieldType.CountDistinctFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                boolean isCountNulls();

                @JsProperty
                void setCountNulls(boolean countNulls);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface DistinctFieldType {
                @JsOverlay
                static AggregateAllRequest.ToObjectReturnType.SpecFieldType.DistinctFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                boolean isIncludeNulls();

                @JsProperty
                void setIncludeNulls(boolean includeNulls);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface FormulaFieldType {
                @JsOverlay
                static AggregateAllRequest.ToObjectReturnType.SpecFieldType.FormulaFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getFormula();

                @JsProperty
                String getParamToken();

                @JsProperty
                void setFormula(String formula);

                @JsProperty
                void setParamToken(String paramToken);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface MedianFieldType {
                @JsOverlay
                static AggregateAllRequest.ToObjectReturnType.SpecFieldType.MedianFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                boolean isAverageEvenlyDivided();

                @JsProperty
                void setAverageEvenlyDivided(boolean averageEvenlyDivided);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface PercentileFieldType {
                @JsOverlay
                static AggregateAllRequest.ToObjectReturnType.SpecFieldType.PercentileFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                double getPercentile();

                @JsProperty
                boolean isAverageEvenlyDivided();

                @JsProperty
                void setAverageEvenlyDivided(boolean averageEvenlyDivided);

                @JsProperty
                void setPercentile(double percentile);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface SortedFirstFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface ColumnsListFieldType {
                    @JsOverlay
                    static AggregateAllRequest.ToObjectReturnType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    String getColumnName();

                    @JsProperty
                    void setColumnName(String columnName);
                }

                @JsOverlay
                static AggregateAllRequest.ToObjectReturnType.SpecFieldType.SortedFirstFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<AggregateAllRequest.ToObjectReturnType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType> getColumnsList();

                @JsOverlay
                default void setColumnsList(
                        AggregateAllRequest.ToObjectReturnType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType[] columnsList) {
                    setColumnsList(
                            Js.<JsArray<AggregateAllRequest.ToObjectReturnType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType>>uncheckedCast(
                                    columnsList));
                }

                @JsProperty
                void setColumnsList(
                        JsArray<AggregateAllRequest.ToObjectReturnType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType> columnsList);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TDigestFieldType {
                @JsOverlay
                static AggregateAllRequest.ToObjectReturnType.SpecFieldType.TDigestFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                double getCompression();

                @JsProperty
                void setCompression(double compression);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface UniqueFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface NonUniqueSentinelFieldType {
                    @JsOverlay
                    static AggregateAllRequest.ToObjectReturnType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    double getByteValue();

                    @JsProperty
                    double getCharValue();

                    @JsProperty
                    double getDoubleValue();

                    @JsProperty
                    double getFloatValue();

                    @JsProperty
                    double getIntValue();

                    @JsProperty
                    String getLongValue();

                    @JsProperty
                    double getNullValue();

                    @JsProperty
                    double getShortValue();

                    @JsProperty
                    String getStringValue();

                    @JsProperty
                    boolean isBoolValue();

                    @JsProperty
                    void setBoolValue(boolean boolValue);

                    @JsProperty
                    void setByteValue(double byteValue);

                    @JsProperty
                    void setCharValue(double charValue);

                    @JsProperty
                    void setDoubleValue(double doubleValue);

                    @JsProperty
                    void setFloatValue(double floatValue);

                    @JsProperty
                    void setIntValue(double intValue);

                    @JsProperty
                    void setLongValue(String longValue);

                    @JsProperty
                    void setNullValue(double nullValue);

                    @JsProperty
                    void setShortValue(double shortValue);

                    @JsProperty
                    void setStringValue(String stringValue);
                }

                @JsOverlay
                static AggregateAllRequest.ToObjectReturnType.SpecFieldType.UniqueFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                AggregateAllRequest.ToObjectReturnType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType getNonUniqueSentinel();

                @JsProperty
                boolean isIncludeNulls();

                @JsProperty
                void setIncludeNulls(boolean includeNulls);

                @JsProperty
                void setNonUniqueSentinel(
                        AggregateAllRequest.ToObjectReturnType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType nonUniqueSentinel);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface WeightedAvgFieldType {
                @JsOverlay
                static AggregateAllRequest.ToObjectReturnType.SpecFieldType.WeightedAvgFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getWeightColumn();

                @JsProperty
                void setWeightColumn(String weightColumn);
            }

            @JsOverlay
            static AggregateAllRequest.ToObjectReturnType.SpecFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            Object getAbsSum();

            @JsProperty
            AggregateAllRequest.ToObjectReturnType.SpecFieldType.ApproximatePercentileFieldType getApproximatePercentile();

            @JsProperty
            Object getAvg();

            @JsProperty
            AggregateAllRequest.ToObjectReturnType.SpecFieldType.CountDistinctFieldType getCountDistinct();

            @JsProperty
            AggregateAllRequest.ToObjectReturnType.SpecFieldType.DistinctFieldType getDistinct();

            @JsProperty
            Object getFirst();

            @JsProperty
            AggregateAllRequest.ToObjectReturnType.SpecFieldType.FormulaFieldType getFormula();

            @JsProperty
            Object getFreeze();

            @JsProperty
            Object getGroup();

            @JsProperty
            Object getLast();

            @JsProperty
            Object getMax();

            @JsProperty
            AggregateAllRequest.ToObjectReturnType.SpecFieldType.MedianFieldType getMedian();

            @JsProperty
            Object getMin();

            @JsProperty
            Object getPb_var();

            @JsProperty
            AggregateAllRequest.ToObjectReturnType.SpecFieldType.PercentileFieldType getPercentile();

            @JsProperty
            AggregateAllRequest.ToObjectReturnType.SpecFieldType.SortedFirstFieldType getSortedFirst();

            @JsProperty
            Object getSortedLast();

            @JsProperty
            Object getStd();

            @JsProperty
            Object getSum();

            @JsProperty
            AggregateAllRequest.ToObjectReturnType.SpecFieldType.TDigestFieldType getTDigest();

            @JsProperty
            AggregateAllRequest.ToObjectReturnType.SpecFieldType.UniqueFieldType getUnique();

            @JsProperty
            AggregateAllRequest.ToObjectReturnType.SpecFieldType.WeightedAvgFieldType getWeightedAvg();

            @JsProperty
            Object getWeightedSum();

            @JsProperty
            void setAbsSum(Object absSum);

            @JsProperty
            void setApproximatePercentile(
                    AggregateAllRequest.ToObjectReturnType.SpecFieldType.ApproximatePercentileFieldType approximatePercentile);

            @JsProperty
            void setAvg(Object avg);

            @JsProperty
            void setCountDistinct(
                    AggregateAllRequest.ToObjectReturnType.SpecFieldType.CountDistinctFieldType countDistinct);

            @JsProperty
            void setDistinct(
                    AggregateAllRequest.ToObjectReturnType.SpecFieldType.DistinctFieldType distinct);

            @JsProperty
            void setFirst(Object first);

            @JsProperty
            void setFormula(
                    AggregateAllRequest.ToObjectReturnType.SpecFieldType.FormulaFieldType formula);

            @JsProperty
            void setFreeze(Object freeze);

            @JsProperty
            void setGroup(Object group);

            @JsProperty
            void setLast(Object last);

            @JsProperty
            void setMax(Object max);

            @JsProperty
            void setMedian(AggregateAllRequest.ToObjectReturnType.SpecFieldType.MedianFieldType median);

            @JsProperty
            void setMin(Object min);

            @JsProperty
            void setPb_var(Object pb_var);

            @JsProperty
            void setPercentile(
                    AggregateAllRequest.ToObjectReturnType.SpecFieldType.PercentileFieldType percentile);

            @JsProperty
            void setSortedFirst(
                    AggregateAllRequest.ToObjectReturnType.SpecFieldType.SortedFirstFieldType sortedFirst);

            @JsProperty
            void setSortedLast(Object sortedLast);

            @JsProperty
            void setStd(Object std);

            @JsProperty
            void setSum(Object sum);

            @JsProperty
            void setTDigest(
                    AggregateAllRequest.ToObjectReturnType.SpecFieldType.TDigestFieldType tDigest);

            @JsProperty
            void setUnique(AggregateAllRequest.ToObjectReturnType.SpecFieldType.UniqueFieldType unique);

            @JsProperty
            void setWeightedAvg(
                    AggregateAllRequest.ToObjectReturnType.SpecFieldType.WeightedAvgFieldType weightedAvg);

            @JsProperty
            void setWeightedSum(Object weightedSum);
        }

        @JsOverlay
        static AggregateAllRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<String> getGroupByColumnsList();

        @JsProperty
        AggregateAllRequest.ToObjectReturnType.ResultIdFieldType getResultId();

        @JsProperty
        AggregateAllRequest.ToObjectReturnType.SourceIdFieldType getSourceId();

        @JsProperty
        AggregateAllRequest.ToObjectReturnType.SpecFieldType getSpec();

        @JsProperty
        void setGroupByColumnsList(JsArray<String> groupByColumnsList);

        @JsOverlay
        default void setGroupByColumnsList(String[] groupByColumnsList) {
            setGroupByColumnsList(Js.<JsArray<String>>uncheckedCast(groupByColumnsList));
        }

        @JsProperty
        void setResultId(AggregateAllRequest.ToObjectReturnType.ResultIdFieldType resultId);

        @JsProperty
        void setSourceId(AggregateAllRequest.ToObjectReturnType.SourceIdFieldType sourceId);

        @JsProperty
        void setSpec(AggregateAllRequest.ToObjectReturnType.SpecFieldType spec);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static AggregateAllRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType of(
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
            static AggregateAllRequest.ToObjectReturnType0.ResultIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            AggregateAllRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    AggregateAllRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<AggregateAllRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<AggregateAllRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SourceIdFieldType {
            @JsOverlay
            static AggregateAllRequest.ToObjectReturnType0.SourceIdFieldType create() {
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

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SpecFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ApproximatePercentileFieldType {
                @JsOverlay
                static AggregateAllRequest.ToObjectReturnType0.SpecFieldType.ApproximatePercentileFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                double getCompression();

                @JsProperty
                double getPercentile();

                @JsProperty
                void setCompression(double compression);

                @JsProperty
                void setPercentile(double percentile);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface CountDistinctFieldType {
                @JsOverlay
                static AggregateAllRequest.ToObjectReturnType0.SpecFieldType.CountDistinctFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                boolean isCountNulls();

                @JsProperty
                void setCountNulls(boolean countNulls);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface DistinctFieldType {
                @JsOverlay
                static AggregateAllRequest.ToObjectReturnType0.SpecFieldType.DistinctFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                boolean isIncludeNulls();

                @JsProperty
                void setIncludeNulls(boolean includeNulls);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface FormulaFieldType {
                @JsOverlay
                static AggregateAllRequest.ToObjectReturnType0.SpecFieldType.FormulaFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getFormula();

                @JsProperty
                String getParamToken();

                @JsProperty
                void setFormula(String formula);

                @JsProperty
                void setParamToken(String paramToken);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface MedianFieldType {
                @JsOverlay
                static AggregateAllRequest.ToObjectReturnType0.SpecFieldType.MedianFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                boolean isAverageEvenlyDivided();

                @JsProperty
                void setAverageEvenlyDivided(boolean averageEvenlyDivided);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface PercentileFieldType {
                @JsOverlay
                static AggregateAllRequest.ToObjectReturnType0.SpecFieldType.PercentileFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                double getPercentile();

                @JsProperty
                boolean isAverageEvenlyDivided();

                @JsProperty
                void setAverageEvenlyDivided(boolean averageEvenlyDivided);

                @JsProperty
                void setPercentile(double percentile);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface SortedFirstFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface ColumnsListFieldType {
                    @JsOverlay
                    static AggregateAllRequest.ToObjectReturnType0.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    String getColumnName();

                    @JsProperty
                    void setColumnName(String columnName);
                }

                @JsOverlay
                static AggregateAllRequest.ToObjectReturnType0.SpecFieldType.SortedFirstFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<AggregateAllRequest.ToObjectReturnType0.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType> getColumnsList();

                @JsOverlay
                default void setColumnsList(
                        AggregateAllRequest.ToObjectReturnType0.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType[] columnsList) {
                    setColumnsList(
                            Js.<JsArray<AggregateAllRequest.ToObjectReturnType0.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType>>uncheckedCast(
                                    columnsList));
                }

                @JsProperty
                void setColumnsList(
                        JsArray<AggregateAllRequest.ToObjectReturnType0.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType> columnsList);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TDigestFieldType {
                @JsOverlay
                static AggregateAllRequest.ToObjectReturnType0.SpecFieldType.TDigestFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                double getCompression();

                @JsProperty
                void setCompression(double compression);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface UniqueFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface NonUniqueSentinelFieldType {
                    @JsOverlay
                    static AggregateAllRequest.ToObjectReturnType0.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    double getByteValue();

                    @JsProperty
                    double getCharValue();

                    @JsProperty
                    double getDoubleValue();

                    @JsProperty
                    double getFloatValue();

                    @JsProperty
                    double getIntValue();

                    @JsProperty
                    String getLongValue();

                    @JsProperty
                    double getNullValue();

                    @JsProperty
                    double getShortValue();

                    @JsProperty
                    String getStringValue();

                    @JsProperty
                    boolean isBoolValue();

                    @JsProperty
                    void setBoolValue(boolean boolValue);

                    @JsProperty
                    void setByteValue(double byteValue);

                    @JsProperty
                    void setCharValue(double charValue);

                    @JsProperty
                    void setDoubleValue(double doubleValue);

                    @JsProperty
                    void setFloatValue(double floatValue);

                    @JsProperty
                    void setIntValue(double intValue);

                    @JsProperty
                    void setLongValue(String longValue);

                    @JsProperty
                    void setNullValue(double nullValue);

                    @JsProperty
                    void setShortValue(double shortValue);

                    @JsProperty
                    void setStringValue(String stringValue);
                }

                @JsOverlay
                static AggregateAllRequest.ToObjectReturnType0.SpecFieldType.UniqueFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                AggregateAllRequest.ToObjectReturnType0.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType getNonUniqueSentinel();

                @JsProperty
                boolean isIncludeNulls();

                @JsProperty
                void setIncludeNulls(boolean includeNulls);

                @JsProperty
                void setNonUniqueSentinel(
                        AggregateAllRequest.ToObjectReturnType0.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType nonUniqueSentinel);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface WeightedAvgFieldType {
                @JsOverlay
                static AggregateAllRequest.ToObjectReturnType0.SpecFieldType.WeightedAvgFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getWeightColumn();

                @JsProperty
                void setWeightColumn(String weightColumn);
            }

            @JsOverlay
            static AggregateAllRequest.ToObjectReturnType0.SpecFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            Object getAbsSum();

            @JsProperty
            AggregateAllRequest.ToObjectReturnType0.SpecFieldType.ApproximatePercentileFieldType getApproximatePercentile();

            @JsProperty
            Object getAvg();

            @JsProperty
            AggregateAllRequest.ToObjectReturnType0.SpecFieldType.CountDistinctFieldType getCountDistinct();

            @JsProperty
            AggregateAllRequest.ToObjectReturnType0.SpecFieldType.DistinctFieldType getDistinct();

            @JsProperty
            Object getFirst();

            @JsProperty
            AggregateAllRequest.ToObjectReturnType0.SpecFieldType.FormulaFieldType getFormula();

            @JsProperty
            Object getFreeze();

            @JsProperty
            Object getGroup();

            @JsProperty
            Object getLast();

            @JsProperty
            Object getMax();

            @JsProperty
            AggregateAllRequest.ToObjectReturnType0.SpecFieldType.MedianFieldType getMedian();

            @JsProperty
            Object getMin();

            @JsProperty
            Object getPb_var();

            @JsProperty
            AggregateAllRequest.ToObjectReturnType0.SpecFieldType.PercentileFieldType getPercentile();

            @JsProperty
            AggregateAllRequest.ToObjectReturnType0.SpecFieldType.SortedFirstFieldType getSortedFirst();

            @JsProperty
            Object getSortedLast();

            @JsProperty
            Object getStd();

            @JsProperty
            Object getSum();

            @JsProperty
            AggregateAllRequest.ToObjectReturnType0.SpecFieldType.TDigestFieldType getTDigest();

            @JsProperty
            AggregateAllRequest.ToObjectReturnType0.SpecFieldType.UniqueFieldType getUnique();

            @JsProperty
            AggregateAllRequest.ToObjectReturnType0.SpecFieldType.WeightedAvgFieldType getWeightedAvg();

            @JsProperty
            Object getWeightedSum();

            @JsProperty
            void setAbsSum(Object absSum);

            @JsProperty
            void setApproximatePercentile(
                    AggregateAllRequest.ToObjectReturnType0.SpecFieldType.ApproximatePercentileFieldType approximatePercentile);

            @JsProperty
            void setAvg(Object avg);

            @JsProperty
            void setCountDistinct(
                    AggregateAllRequest.ToObjectReturnType0.SpecFieldType.CountDistinctFieldType countDistinct);

            @JsProperty
            void setDistinct(
                    AggregateAllRequest.ToObjectReturnType0.SpecFieldType.DistinctFieldType distinct);

            @JsProperty
            void setFirst(Object first);

            @JsProperty
            void setFormula(
                    AggregateAllRequest.ToObjectReturnType0.SpecFieldType.FormulaFieldType formula);

            @JsProperty
            void setFreeze(Object freeze);

            @JsProperty
            void setGroup(Object group);

            @JsProperty
            void setLast(Object last);

            @JsProperty
            void setMax(Object max);

            @JsProperty
            void setMedian(AggregateAllRequest.ToObjectReturnType0.SpecFieldType.MedianFieldType median);

            @JsProperty
            void setMin(Object min);

            @JsProperty
            void setPb_var(Object pb_var);

            @JsProperty
            void setPercentile(
                    AggregateAllRequest.ToObjectReturnType0.SpecFieldType.PercentileFieldType percentile);

            @JsProperty
            void setSortedFirst(
                    AggregateAllRequest.ToObjectReturnType0.SpecFieldType.SortedFirstFieldType sortedFirst);

            @JsProperty
            void setSortedLast(Object sortedLast);

            @JsProperty
            void setStd(Object std);

            @JsProperty
            void setSum(Object sum);

            @JsProperty
            void setTDigest(
                    AggregateAllRequest.ToObjectReturnType0.SpecFieldType.TDigestFieldType tDigest);

            @JsProperty
            void setUnique(AggregateAllRequest.ToObjectReturnType0.SpecFieldType.UniqueFieldType unique);

            @JsProperty
            void setWeightedAvg(
                    AggregateAllRequest.ToObjectReturnType0.SpecFieldType.WeightedAvgFieldType weightedAvg);

            @JsProperty
            void setWeightedSum(Object weightedSum);
        }

        @JsOverlay
        static AggregateAllRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<String> getGroupByColumnsList();

        @JsProperty
        AggregateAllRequest.ToObjectReturnType0.ResultIdFieldType getResultId();

        @JsProperty
        AggregateAllRequest.ToObjectReturnType0.SourceIdFieldType getSourceId();

        @JsProperty
        AggregateAllRequest.ToObjectReturnType0.SpecFieldType getSpec();

        @JsProperty
        void setGroupByColumnsList(JsArray<String> groupByColumnsList);

        @JsOverlay
        default void setGroupByColumnsList(String[] groupByColumnsList) {
            setGroupByColumnsList(Js.<JsArray<String>>uncheckedCast(groupByColumnsList));
        }

        @JsProperty
        void setResultId(AggregateAllRequest.ToObjectReturnType0.ResultIdFieldType resultId);

        @JsProperty
        void setSourceId(AggregateAllRequest.ToObjectReturnType0.SourceIdFieldType sourceId);

        @JsProperty
        void setSpec(AggregateAllRequest.ToObjectReturnType0.SpecFieldType spec);
    }

    public static native AggregateAllRequest deserializeBinary(Uint8Array bytes);

    public static native AggregateAllRequest deserializeBinaryFromReader(
            AggregateAllRequest message, Object reader);

    public static native void serializeBinaryToWriter(AggregateAllRequest message, Object writer);

    public static native AggregateAllRequest.ToObjectReturnType toObject(
            boolean includeInstance, AggregateAllRequest msg);

    public native String addGroupByColumns(String value, double index);

    public native String addGroupByColumns(String value);

    public native void clearGroupByColumnsList();

    public native void clearResultId();

    public native void clearSourceId();

    public native void clearSpec();

    public native JsArray<String> getGroupByColumnsList();

    public native Ticket getResultId();

    public native TableReference getSourceId();

    public native AggSpec getSpec();

    public native boolean hasResultId();

    public native boolean hasSourceId();

    public native boolean hasSpec();

    public native Uint8Array serializeBinary();

    public native void setGroupByColumnsList(JsArray<String> value);

    @JsOverlay
    public final void setGroupByColumnsList(String[] value) {
        setGroupByColumnsList(Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void setResultId();

    public native void setResultId(Ticket value);

    public native void setSourceId();

    public native void setSourceId(TableReference value);

    public native void setSpec();

    public native void setSpec(AggSpec value);

    public native AggregateAllRequest.ToObjectReturnType0 toObject();

    public native AggregateAllRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
