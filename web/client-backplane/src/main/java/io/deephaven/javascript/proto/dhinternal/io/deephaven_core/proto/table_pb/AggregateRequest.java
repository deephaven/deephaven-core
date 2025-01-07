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
        name = "dhinternal.io.deephaven_core.proto.table_pb.AggregateRequest",
        namespace = JsPackage.GLOBAL)
public class AggregateRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface AggregationsListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ColumnsFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface SpecFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface ApproximatePercentileFieldType {
                        @JsOverlay
                        static AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.ApproximatePercentileFieldType create() {
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
                        static AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.CountDistinctFieldType create() {
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
                        static AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.DistinctFieldType create() {
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
                        static AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.FormulaFieldType create() {
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
                        static AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.MedianFieldType create() {
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
                        static AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.PercentileFieldType create() {
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
                            static AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType create() {
                                return Js.uncheckedCast(JsPropertyMap.of());
                            }

                            @JsProperty
                            String getColumnName();

                            @JsProperty
                            void setColumnName(String columnName);
                        }

                        @JsOverlay
                        static AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        JsArray<AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType> getColumnsList();

                        @JsOverlay
                        default void setColumnsList(
                                AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType[] columnsList) {
                            setColumnsList(
                                    Js.<JsArray<AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType>>uncheckedCast(
                                            columnsList));
                        }

                        @JsProperty
                        void setColumnsList(
                                JsArray<AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType> columnsList);
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface TDigestFieldType {
                        @JsOverlay
                        static AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.TDigestFieldType create() {
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
                            static AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType create() {
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
                        static AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType getNonUniqueSentinel();

                        @JsProperty
                        boolean isIncludeNulls();

                        @JsProperty
                        void setIncludeNulls(boolean includeNulls);

                        @JsProperty
                        void setNonUniqueSentinel(
                                AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType nonUniqueSentinel);
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface WeightedAvgFieldType {
                        @JsOverlay
                        static AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.WeightedAvgFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        String getWeightColumn();

                        @JsProperty
                        void setWeightColumn(String weightColumn);
                    }

                    @JsOverlay
                    static AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    Object getAbsSum();

                    @JsProperty
                    AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.ApproximatePercentileFieldType getApproximatePercentile();

                    @JsProperty
                    Object getAvg();

                    @JsProperty
                    AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.CountDistinctFieldType getCountDistinct();

                    @JsProperty
                    AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.DistinctFieldType getDistinct();

                    @JsProperty
                    Object getFirst();

                    @JsProperty
                    AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.FormulaFieldType getFormula();

                    @JsProperty
                    Object getFreeze();

                    @JsProperty
                    Object getGroup();

                    @JsProperty
                    Object getLast();

                    @JsProperty
                    Object getMax();

                    @JsProperty
                    AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.MedianFieldType getMedian();

                    @JsProperty
                    Object getMin();

                    @JsProperty
                    Object getPb_var();

                    @JsProperty
                    AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.PercentileFieldType getPercentile();

                    @JsProperty
                    AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType getSortedFirst();

                    @JsProperty
                    Object getSortedLast();

                    @JsProperty
                    Object getStd();

                    @JsProperty
                    Object getSum();

                    @JsProperty
                    AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.TDigestFieldType getTDigest();

                    @JsProperty
                    AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType getUnique();

                    @JsProperty
                    AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.WeightedAvgFieldType getWeightedAvg();

                    @JsProperty
                    Object getWeightedSum();

                    @JsProperty
                    void setAbsSum(Object absSum);

                    @JsProperty
                    void setApproximatePercentile(
                            AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.ApproximatePercentileFieldType approximatePercentile);

                    @JsProperty
                    void setAvg(Object avg);

                    @JsProperty
                    void setCountDistinct(
                            AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.CountDistinctFieldType countDistinct);

                    @JsProperty
                    void setDistinct(
                            AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.DistinctFieldType distinct);

                    @JsProperty
                    void setFirst(Object first);

                    @JsProperty
                    void setFormula(
                            AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.FormulaFieldType formula);

                    @JsProperty
                    void setFreeze(Object freeze);

                    @JsProperty
                    void setGroup(Object group);

                    @JsProperty
                    void setLast(Object last);

                    @JsProperty
                    void setMax(Object max);

                    @JsProperty
                    void setMedian(
                            AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.MedianFieldType median);

                    @JsProperty
                    void setMin(Object min);

                    @JsProperty
                    void setPb_var(Object pb_var);

                    @JsProperty
                    void setPercentile(
                            AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.PercentileFieldType percentile);

                    @JsProperty
                    void setSortedFirst(
                            AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType sortedFirst);

                    @JsProperty
                    void setSortedLast(Object sortedLast);

                    @JsProperty
                    void setStd(Object std);

                    @JsProperty
                    void setSum(Object sum);

                    @JsProperty
                    void setTDigest(
                            AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.TDigestFieldType tDigest);

                    @JsProperty
                    void setUnique(
                            AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType unique);

                    @JsProperty
                    void setWeightedAvg(
                            AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.WeightedAvgFieldType weightedAvg);

                    @JsProperty
                    void setWeightedSum(Object weightedSum);
                }

                @JsOverlay
                static AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<String> getMatchPairsList();

                @JsProperty
                AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType getSpec();

                @JsProperty
                void setMatchPairsList(JsArray<String> matchPairsList);

                @JsOverlay
                default void setMatchPairsList(String[] matchPairsList) {
                    setMatchPairsList(Js.<JsArray<String>>uncheckedCast(matchPairsList));
                }

                @JsProperty
                void setSpec(
                        AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType spec);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface CountFieldType {
                @JsOverlay
                static AggregateRequest.ToObjectReturnType.AggregationsListFieldType.CountFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getColumnName();

                @JsProperty
                void setColumnName(String columnName);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface FirstRowKeyFieldType {
                @JsOverlay
                static AggregateRequest.ToObjectReturnType.AggregationsListFieldType.FirstRowKeyFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getColumnName();

                @JsProperty
                void setColumnName(String columnName);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface PartitionFieldType {
                @JsOverlay
                static AggregateRequest.ToObjectReturnType.AggregationsListFieldType.PartitionFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getColumnName();

                @JsProperty
                boolean isIncludeGroupByColumns();

                @JsProperty
                void setColumnName(String columnName);

                @JsProperty
                void setIncludeGroupByColumns(boolean includeGroupByColumns);
            }

            @JsOverlay
            static AggregateRequest.ToObjectReturnType.AggregationsListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType getColumns();

            @JsProperty
            AggregateRequest.ToObjectReturnType.AggregationsListFieldType.CountFieldType getCount();

            @JsProperty
            AggregateRequest.ToObjectReturnType.AggregationsListFieldType.FirstRowKeyFieldType getFirstRowKey();

            @JsProperty
            Object getLastRowKey();

            @JsProperty
            AggregateRequest.ToObjectReturnType.AggregationsListFieldType.PartitionFieldType getPartition();

            @JsProperty
            void setColumns(
                    AggregateRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType columns);

            @JsProperty
            void setCount(
                    AggregateRequest.ToObjectReturnType.AggregationsListFieldType.CountFieldType count);

            @JsProperty
            void setFirstRowKey(
                    AggregateRequest.ToObjectReturnType.AggregationsListFieldType.FirstRowKeyFieldType firstRowKey);

            @JsProperty
            void setLastRowKey(Object lastRowKey);

            @JsProperty
            void setPartition(
                    AggregateRequest.ToObjectReturnType.AggregationsListFieldType.PartitionFieldType partition);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static AggregateRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType of(
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
            static AggregateRequest.ToObjectReturnType.ResultIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            AggregateRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    AggregateRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<AggregateRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<AggregateRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SourceIdFieldType {
            @JsOverlay
            static AggregateRequest.ToObjectReturnType.SourceIdFieldType create() {
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
        static AggregateRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<AggregateRequest.ToObjectReturnType.AggregationsListFieldType> getAggregationsList();

        @JsProperty
        JsArray<String> getGroupByColumnsList();

        @JsProperty
        Object getInitialGroupsId();

        @JsProperty
        AggregateRequest.ToObjectReturnType.ResultIdFieldType getResultId();

        @JsProperty
        AggregateRequest.ToObjectReturnType.SourceIdFieldType getSourceId();

        @JsProperty
        boolean isPreserveEmpty();

        @JsOverlay
        default void setAggregationsList(
                AggregateRequest.ToObjectReturnType.AggregationsListFieldType[] aggregationsList) {
            setAggregationsList(
                    Js.<JsArray<AggregateRequest.ToObjectReturnType.AggregationsListFieldType>>uncheckedCast(
                            aggregationsList));
        }

        @JsProperty
        void setAggregationsList(
                JsArray<AggregateRequest.ToObjectReturnType.AggregationsListFieldType> aggregationsList);

        @JsProperty
        void setGroupByColumnsList(JsArray<String> groupByColumnsList);

        @JsOverlay
        default void setGroupByColumnsList(String[] groupByColumnsList) {
            setGroupByColumnsList(Js.<JsArray<String>>uncheckedCast(groupByColumnsList));
        }

        @JsProperty
        void setInitialGroupsId(Object initialGroupsId);

        @JsProperty
        void setPreserveEmpty(boolean preserveEmpty);

        @JsProperty
        void setResultId(AggregateRequest.ToObjectReturnType.ResultIdFieldType resultId);

        @JsProperty
        void setSourceId(AggregateRequest.ToObjectReturnType.SourceIdFieldType sourceId);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface AggregationsListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ColumnsFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface SpecFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface ApproximatePercentileFieldType {
                        @JsOverlay
                        static AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.ApproximatePercentileFieldType create() {
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
                        static AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.CountDistinctFieldType create() {
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
                        static AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.DistinctFieldType create() {
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
                        static AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.FormulaFieldType create() {
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
                        static AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.MedianFieldType create() {
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
                        static AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.PercentileFieldType create() {
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
                            static AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType create() {
                                return Js.uncheckedCast(JsPropertyMap.of());
                            }

                            @JsProperty
                            String getColumnName();

                            @JsProperty
                            void setColumnName(String columnName);
                        }

                        @JsOverlay
                        static AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        JsArray<AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType> getColumnsList();

                        @JsOverlay
                        default void setColumnsList(
                                AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType[] columnsList) {
                            setColumnsList(
                                    Js.<JsArray<AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType>>uncheckedCast(
                                            columnsList));
                        }

                        @JsProperty
                        void setColumnsList(
                                JsArray<AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType> columnsList);
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface TDigestFieldType {
                        @JsOverlay
                        static AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.TDigestFieldType create() {
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
                            static AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType create() {
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
                        static AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType getNonUniqueSentinel();

                        @JsProperty
                        boolean isIncludeNulls();

                        @JsProperty
                        void setIncludeNulls(boolean includeNulls);

                        @JsProperty
                        void setNonUniqueSentinel(
                                AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType nonUniqueSentinel);
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface WeightedAvgFieldType {
                        @JsOverlay
                        static AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.WeightedAvgFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        String getWeightColumn();

                        @JsProperty
                        void setWeightColumn(String weightColumn);
                    }

                    @JsOverlay
                    static AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    Object getAbsSum();

                    @JsProperty
                    AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.ApproximatePercentileFieldType getApproximatePercentile();

                    @JsProperty
                    Object getAvg();

                    @JsProperty
                    AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.CountDistinctFieldType getCountDistinct();

                    @JsProperty
                    AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.DistinctFieldType getDistinct();

                    @JsProperty
                    Object getFirst();

                    @JsProperty
                    AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.FormulaFieldType getFormula();

                    @JsProperty
                    Object getFreeze();

                    @JsProperty
                    Object getGroup();

                    @JsProperty
                    Object getLast();

                    @JsProperty
                    Object getMax();

                    @JsProperty
                    AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.MedianFieldType getMedian();

                    @JsProperty
                    Object getMin();

                    @JsProperty
                    Object getPb_var();

                    @JsProperty
                    AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.PercentileFieldType getPercentile();

                    @JsProperty
                    AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType getSortedFirst();

                    @JsProperty
                    Object getSortedLast();

                    @JsProperty
                    Object getStd();

                    @JsProperty
                    Object getSum();

                    @JsProperty
                    AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.TDigestFieldType getTDigest();

                    @JsProperty
                    AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType getUnique();

                    @JsProperty
                    AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.WeightedAvgFieldType getWeightedAvg();

                    @JsProperty
                    Object getWeightedSum();

                    @JsProperty
                    void setAbsSum(Object absSum);

                    @JsProperty
                    void setApproximatePercentile(
                            AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.ApproximatePercentileFieldType approximatePercentile);

                    @JsProperty
                    void setAvg(Object avg);

                    @JsProperty
                    void setCountDistinct(
                            AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.CountDistinctFieldType countDistinct);

                    @JsProperty
                    void setDistinct(
                            AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.DistinctFieldType distinct);

                    @JsProperty
                    void setFirst(Object first);

                    @JsProperty
                    void setFormula(
                            AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.FormulaFieldType formula);

                    @JsProperty
                    void setFreeze(Object freeze);

                    @JsProperty
                    void setGroup(Object group);

                    @JsProperty
                    void setLast(Object last);

                    @JsProperty
                    void setMax(Object max);

                    @JsProperty
                    void setMedian(
                            AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.MedianFieldType median);

                    @JsProperty
                    void setMin(Object min);

                    @JsProperty
                    void setPb_var(Object pb_var);

                    @JsProperty
                    void setPercentile(
                            AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.PercentileFieldType percentile);

                    @JsProperty
                    void setSortedFirst(
                            AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType sortedFirst);

                    @JsProperty
                    void setSortedLast(Object sortedLast);

                    @JsProperty
                    void setStd(Object std);

                    @JsProperty
                    void setSum(Object sum);

                    @JsProperty
                    void setTDigest(
                            AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.TDigestFieldType tDigest);

                    @JsProperty
                    void setUnique(
                            AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType unique);

                    @JsProperty
                    void setWeightedAvg(
                            AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.WeightedAvgFieldType weightedAvg);

                    @JsProperty
                    void setWeightedSum(Object weightedSum);
                }

                @JsOverlay
                static AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<String> getMatchPairsList();

                @JsProperty
                AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType getSpec();

                @JsProperty
                void setMatchPairsList(JsArray<String> matchPairsList);

                @JsOverlay
                default void setMatchPairsList(String[] matchPairsList) {
                    setMatchPairsList(Js.<JsArray<String>>uncheckedCast(matchPairsList));
                }

                @JsProperty
                void setSpec(
                        AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType spec);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface CountFieldType {
                @JsOverlay
                static AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.CountFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getColumnName();

                @JsProperty
                void setColumnName(String columnName);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface FirstRowKeyFieldType {
                @JsOverlay
                static AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.FirstRowKeyFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getColumnName();

                @JsProperty
                void setColumnName(String columnName);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface PartitionFieldType {
                @JsOverlay
                static AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.PartitionFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getColumnName();

                @JsProperty
                boolean isIncludeGroupByColumns();

                @JsProperty
                void setColumnName(String columnName);

                @JsProperty
                void setIncludeGroupByColumns(boolean includeGroupByColumns);
            }

            @JsOverlay
            static AggregateRequest.ToObjectReturnType0.AggregationsListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType getColumns();

            @JsProperty
            AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.CountFieldType getCount();

            @JsProperty
            AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.FirstRowKeyFieldType getFirstRowKey();

            @JsProperty
            Object getLastRowKey();

            @JsProperty
            AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.PartitionFieldType getPartition();

            @JsProperty
            void setColumns(
                    AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType columns);

            @JsProperty
            void setCount(
                    AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.CountFieldType count);

            @JsProperty
            void setFirstRowKey(
                    AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.FirstRowKeyFieldType firstRowKey);

            @JsProperty
            void setLastRowKey(Object lastRowKey);

            @JsProperty
            void setPartition(
                    AggregateRequest.ToObjectReturnType0.AggregationsListFieldType.PartitionFieldType partition);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static AggregateRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType of(
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
            static AggregateRequest.ToObjectReturnType0.ResultIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            AggregateRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    AggregateRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<AggregateRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<AggregateRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SourceIdFieldType {
            @JsOverlay
            static AggregateRequest.ToObjectReturnType0.SourceIdFieldType create() {
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
        static AggregateRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<AggregateRequest.ToObjectReturnType0.AggregationsListFieldType> getAggregationsList();

        @JsProperty
        JsArray<String> getGroupByColumnsList();

        @JsProperty
        Object getInitialGroupsId();

        @JsProperty
        AggregateRequest.ToObjectReturnType0.ResultIdFieldType getResultId();

        @JsProperty
        AggregateRequest.ToObjectReturnType0.SourceIdFieldType getSourceId();

        @JsProperty
        boolean isPreserveEmpty();

        @JsOverlay
        default void setAggregationsList(
                AggregateRequest.ToObjectReturnType0.AggregationsListFieldType[] aggregationsList) {
            setAggregationsList(
                    Js.<JsArray<AggregateRequest.ToObjectReturnType0.AggregationsListFieldType>>uncheckedCast(
                            aggregationsList));
        }

        @JsProperty
        void setAggregationsList(
                JsArray<AggregateRequest.ToObjectReturnType0.AggregationsListFieldType> aggregationsList);

        @JsProperty
        void setGroupByColumnsList(JsArray<String> groupByColumnsList);

        @JsOverlay
        default void setGroupByColumnsList(String[] groupByColumnsList) {
            setGroupByColumnsList(Js.<JsArray<String>>uncheckedCast(groupByColumnsList));
        }

        @JsProperty
        void setInitialGroupsId(Object initialGroupsId);

        @JsProperty
        void setPreserveEmpty(boolean preserveEmpty);

        @JsProperty
        void setResultId(AggregateRequest.ToObjectReturnType0.ResultIdFieldType resultId);

        @JsProperty
        void setSourceId(AggregateRequest.ToObjectReturnType0.SourceIdFieldType sourceId);
    }

    public static native AggregateRequest deserializeBinary(Uint8Array bytes);

    public static native AggregateRequest deserializeBinaryFromReader(
            AggregateRequest message, Object reader);

    public static native void serializeBinaryToWriter(AggregateRequest message, Object writer);

    public static native AggregateRequest.ToObjectReturnType toObject(
            boolean includeInstance, AggregateRequest msg);

    public native Aggregation addAggregations();

    public native Aggregation addAggregations(Aggregation value, double index);

    public native Aggregation addAggregations(Aggregation value);

    public native String addGroupByColumns(String value, double index);

    public native String addGroupByColumns(String value);

    public native void clearAggregationsList();

    public native void clearGroupByColumnsList();

    public native void clearInitialGroupsId();

    public native void clearResultId();

    public native void clearSourceId();

    public native JsArray<Aggregation> getAggregationsList();

    public native JsArray<String> getGroupByColumnsList();

    public native TableReference getInitialGroupsId();

    public native boolean getPreserveEmpty();

    public native Ticket getResultId();

    public native TableReference getSourceId();

    public native boolean hasInitialGroupsId();

    public native boolean hasResultId();

    public native boolean hasSourceId();

    public native Uint8Array serializeBinary();

    @JsOverlay
    public final void setAggregationsList(Aggregation[] value) {
        setAggregationsList(Js.<JsArray<Aggregation>>uncheckedCast(value));
    }

    public native void setAggregationsList(JsArray<Aggregation> value);

    public native void setGroupByColumnsList(JsArray<String> value);

    @JsOverlay
    public final void setGroupByColumnsList(String[] value) {
        setGroupByColumnsList(Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void setInitialGroupsId();

    public native void setInitialGroupsId(TableReference value);

    public native void setPreserveEmpty(boolean value);

    public native void setResultId();

    public native void setResultId(Ticket value);

    public native void setSourceId();

    public native void setSourceId(TableReference value);

    public native AggregateRequest.ToObjectReturnType0 toObject();

    public native AggregateRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
