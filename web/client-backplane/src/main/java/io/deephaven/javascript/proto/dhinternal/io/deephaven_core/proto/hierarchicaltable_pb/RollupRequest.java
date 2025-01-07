//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.hierarchicaltable_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.Aggregation;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.ticket_pb.Ticket;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.hierarchicaltable_pb.RollupRequest",
        namespace = JsPackage.GLOBAL)
public class RollupRequest {
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
                        static RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.ApproximatePercentileFieldType create() {
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
                        static RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.CountDistinctFieldType create() {
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
                        static RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.DistinctFieldType create() {
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
                        static RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.FormulaFieldType create() {
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
                        static RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.MedianFieldType create() {
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
                        static RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.PercentileFieldType create() {
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
                            static RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType create() {
                                return Js.uncheckedCast(JsPropertyMap.of());
                            }

                            @JsProperty
                            String getColumnName();

                            @JsProperty
                            void setColumnName(String columnName);
                        }

                        @JsOverlay
                        static RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        JsArray<RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType> getColumnsList();

                        @JsOverlay
                        default void setColumnsList(
                                RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType[] columnsList) {
                            setColumnsList(
                                    Js.<JsArray<RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType>>uncheckedCast(
                                            columnsList));
                        }

                        @JsProperty
                        void setColumnsList(
                                JsArray<RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType> columnsList);
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface TDigestFieldType {
                        @JsOverlay
                        static RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.TDigestFieldType create() {
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
                            static RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType create() {
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
                        static RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType getNonUniqueSentinel();

                        @JsProperty
                        boolean isIncludeNulls();

                        @JsProperty
                        void setIncludeNulls(boolean includeNulls);

                        @JsProperty
                        void setNonUniqueSentinel(
                                RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType nonUniqueSentinel);
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface WeightedAvgFieldType {
                        @JsOverlay
                        static RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.WeightedAvgFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        String getWeightColumn();

                        @JsProperty
                        void setWeightColumn(String weightColumn);
                    }

                    @JsOverlay
                    static RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    Object getAbsSum();

                    @JsProperty
                    RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.ApproximatePercentileFieldType getApproximatePercentile();

                    @JsProperty
                    Object getAvg();

                    @JsProperty
                    RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.CountDistinctFieldType getCountDistinct();

                    @JsProperty
                    RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.DistinctFieldType getDistinct();

                    @JsProperty
                    Object getFirst();

                    @JsProperty
                    RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.FormulaFieldType getFormula();

                    @JsProperty
                    Object getFreeze();

                    @JsProperty
                    Object getGroup();

                    @JsProperty
                    Object getLast();

                    @JsProperty
                    Object getMax();

                    @JsProperty
                    RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.MedianFieldType getMedian();

                    @JsProperty
                    Object getMin();

                    @JsProperty
                    Object getPb_var();

                    @JsProperty
                    RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.PercentileFieldType getPercentile();

                    @JsProperty
                    RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType getSortedFirst();

                    @JsProperty
                    Object getSortedLast();

                    @JsProperty
                    Object getStd();

                    @JsProperty
                    Object getSum();

                    @JsProperty
                    RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.TDigestFieldType getTDigest();

                    @JsProperty
                    RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType getUnique();

                    @JsProperty
                    RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.WeightedAvgFieldType getWeightedAvg();

                    @JsProperty
                    Object getWeightedSum();

                    @JsProperty
                    void setAbsSum(Object absSum);

                    @JsProperty
                    void setApproximatePercentile(
                            RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.ApproximatePercentileFieldType approximatePercentile);

                    @JsProperty
                    void setAvg(Object avg);

                    @JsProperty
                    void setCountDistinct(
                            RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.CountDistinctFieldType countDistinct);

                    @JsProperty
                    void setDistinct(
                            RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.DistinctFieldType distinct);

                    @JsProperty
                    void setFirst(Object first);

                    @JsProperty
                    void setFormula(
                            RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.FormulaFieldType formula);

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
                            RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.MedianFieldType median);

                    @JsProperty
                    void setMin(Object min);

                    @JsProperty
                    void setPb_var(Object pb_var);

                    @JsProperty
                    void setPercentile(
                            RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.PercentileFieldType percentile);

                    @JsProperty
                    void setSortedFirst(
                            RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType sortedFirst);

                    @JsProperty
                    void setSortedLast(Object sortedLast);

                    @JsProperty
                    void setStd(Object std);

                    @JsProperty
                    void setSum(Object sum);

                    @JsProperty
                    void setTDigest(
                            RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.TDigestFieldType tDigest);

                    @JsProperty
                    void setUnique(
                            RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType unique);

                    @JsProperty
                    void setWeightedAvg(
                            RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.WeightedAvgFieldType weightedAvg);

                    @JsProperty
                    void setWeightedSum(Object weightedSum);
                }

                @JsOverlay
                static RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<String> getMatchPairsList();

                @JsProperty
                RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType getSpec();

                @JsProperty
                void setMatchPairsList(JsArray<String> matchPairsList);

                @JsOverlay
                default void setMatchPairsList(String[] matchPairsList) {
                    setMatchPairsList(Js.<JsArray<String>>uncheckedCast(matchPairsList));
                }

                @JsProperty
                void setSpec(
                        RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType spec);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface CountFieldType {
                @JsOverlay
                static RollupRequest.ToObjectReturnType.AggregationsListFieldType.CountFieldType create() {
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
                static RollupRequest.ToObjectReturnType.AggregationsListFieldType.FirstRowKeyFieldType create() {
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
                static RollupRequest.ToObjectReturnType.AggregationsListFieldType.PartitionFieldType create() {
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
            static RollupRequest.ToObjectReturnType.AggregationsListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType getColumns();

            @JsProperty
            RollupRequest.ToObjectReturnType.AggregationsListFieldType.CountFieldType getCount();

            @JsProperty
            RollupRequest.ToObjectReturnType.AggregationsListFieldType.FirstRowKeyFieldType getFirstRowKey();

            @JsProperty
            Object getLastRowKey();

            @JsProperty
            RollupRequest.ToObjectReturnType.AggregationsListFieldType.PartitionFieldType getPartition();

            @JsProperty
            void setColumns(
                    RollupRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType columns);

            @JsProperty
            void setCount(
                    RollupRequest.ToObjectReturnType.AggregationsListFieldType.CountFieldType count);

            @JsProperty
            void setFirstRowKey(
                    RollupRequest.ToObjectReturnType.AggregationsListFieldType.FirstRowKeyFieldType firstRowKey);

            @JsProperty
            void setLastRowKey(Object lastRowKey);

            @JsProperty
            void setPartition(
                    RollupRequest.ToObjectReturnType.AggregationsListFieldType.PartitionFieldType partition);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultRollupTableIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static RollupRequest.ToObjectReturnType.ResultRollupTableIdFieldType.GetTicketUnionType of(
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
            static RollupRequest.ToObjectReturnType.ResultRollupTableIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            RollupRequest.ToObjectReturnType.ResultRollupTableIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    RollupRequest.ToObjectReturnType.ResultRollupTableIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<RollupRequest.ToObjectReturnType.ResultRollupTableIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<RollupRequest.ToObjectReturnType.ResultRollupTableIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsOverlay
        static RollupRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<RollupRequest.ToObjectReturnType.AggregationsListFieldType> getAggregationsList();

        @JsProperty
        JsArray<String> getGroupByColumnsList();

        @JsProperty
        RollupRequest.ToObjectReturnType.ResultRollupTableIdFieldType getResultRollupTableId();

        @JsProperty
        Object getSourceTableId();

        @JsProperty
        boolean isIncludeConstituents();

        @JsOverlay
        default void setAggregationsList(
                RollupRequest.ToObjectReturnType.AggregationsListFieldType[] aggregationsList) {
            setAggregationsList(
                    Js.<JsArray<RollupRequest.ToObjectReturnType.AggregationsListFieldType>>uncheckedCast(
                            aggregationsList));
        }

        @JsProperty
        void setAggregationsList(
                JsArray<RollupRequest.ToObjectReturnType.AggregationsListFieldType> aggregationsList);

        @JsProperty
        void setGroupByColumnsList(JsArray<String> groupByColumnsList);

        @JsOverlay
        default void setGroupByColumnsList(String[] groupByColumnsList) {
            setGroupByColumnsList(Js.<JsArray<String>>uncheckedCast(groupByColumnsList));
        }

        @JsProperty
        void setIncludeConstituents(boolean includeConstituents);

        @JsProperty
        void setResultRollupTableId(
                RollupRequest.ToObjectReturnType.ResultRollupTableIdFieldType resultRollupTableId);

        @JsProperty
        void setSourceTableId(Object sourceTableId);
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
                        static RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.ApproximatePercentileFieldType create() {
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
                        static RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.CountDistinctFieldType create() {
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
                        static RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.DistinctFieldType create() {
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
                        static RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.FormulaFieldType create() {
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
                        static RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.MedianFieldType create() {
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
                        static RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.PercentileFieldType create() {
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
                            static RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType create() {
                                return Js.uncheckedCast(JsPropertyMap.of());
                            }

                            @JsProperty
                            String getColumnName();

                            @JsProperty
                            void setColumnName(String columnName);
                        }

                        @JsOverlay
                        static RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        JsArray<RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType> getColumnsList();

                        @JsOverlay
                        default void setColumnsList(
                                RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType[] columnsList) {
                            setColumnsList(
                                    Js.<JsArray<RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType>>uncheckedCast(
                                            columnsList));
                        }

                        @JsProperty
                        void setColumnsList(
                                JsArray<RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType> columnsList);
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface TDigestFieldType {
                        @JsOverlay
                        static RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.TDigestFieldType create() {
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
                            static RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType create() {
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
                        static RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType getNonUniqueSentinel();

                        @JsProperty
                        boolean isIncludeNulls();

                        @JsProperty
                        void setIncludeNulls(boolean includeNulls);

                        @JsProperty
                        void setNonUniqueSentinel(
                                RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType nonUniqueSentinel);
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface WeightedAvgFieldType {
                        @JsOverlay
                        static RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.WeightedAvgFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        String getWeightColumn();

                        @JsProperty
                        void setWeightColumn(String weightColumn);
                    }

                    @JsOverlay
                    static RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    Object getAbsSum();

                    @JsProperty
                    RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.ApproximatePercentileFieldType getApproximatePercentile();

                    @JsProperty
                    Object getAvg();

                    @JsProperty
                    RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.CountDistinctFieldType getCountDistinct();

                    @JsProperty
                    RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.DistinctFieldType getDistinct();

                    @JsProperty
                    Object getFirst();

                    @JsProperty
                    RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.FormulaFieldType getFormula();

                    @JsProperty
                    Object getFreeze();

                    @JsProperty
                    Object getGroup();

                    @JsProperty
                    Object getLast();

                    @JsProperty
                    Object getMax();

                    @JsProperty
                    RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.MedianFieldType getMedian();

                    @JsProperty
                    Object getMin();

                    @JsProperty
                    Object getPb_var();

                    @JsProperty
                    RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.PercentileFieldType getPercentile();

                    @JsProperty
                    RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType getSortedFirst();

                    @JsProperty
                    Object getSortedLast();

                    @JsProperty
                    Object getStd();

                    @JsProperty
                    Object getSum();

                    @JsProperty
                    RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.TDigestFieldType getTDigest();

                    @JsProperty
                    RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType getUnique();

                    @JsProperty
                    RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.WeightedAvgFieldType getWeightedAvg();

                    @JsProperty
                    Object getWeightedSum();

                    @JsProperty
                    void setAbsSum(Object absSum);

                    @JsProperty
                    void setApproximatePercentile(
                            RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.ApproximatePercentileFieldType approximatePercentile);

                    @JsProperty
                    void setAvg(Object avg);

                    @JsProperty
                    void setCountDistinct(
                            RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.CountDistinctFieldType countDistinct);

                    @JsProperty
                    void setDistinct(
                            RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.DistinctFieldType distinct);

                    @JsProperty
                    void setFirst(Object first);

                    @JsProperty
                    void setFormula(
                            RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.FormulaFieldType formula);

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
                            RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.MedianFieldType median);

                    @JsProperty
                    void setMin(Object min);

                    @JsProperty
                    void setPb_var(Object pb_var);

                    @JsProperty
                    void setPercentile(
                            RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.PercentileFieldType percentile);

                    @JsProperty
                    void setSortedFirst(
                            RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType sortedFirst);

                    @JsProperty
                    void setSortedLast(Object sortedLast);

                    @JsProperty
                    void setStd(Object std);

                    @JsProperty
                    void setSum(Object sum);

                    @JsProperty
                    void setTDigest(
                            RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.TDigestFieldType tDigest);

                    @JsProperty
                    void setUnique(
                            RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType unique);

                    @JsProperty
                    void setWeightedAvg(
                            RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.WeightedAvgFieldType weightedAvg);

                    @JsProperty
                    void setWeightedSum(Object weightedSum);
                }

                @JsOverlay
                static RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<String> getMatchPairsList();

                @JsProperty
                RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType getSpec();

                @JsProperty
                void setMatchPairsList(JsArray<String> matchPairsList);

                @JsOverlay
                default void setMatchPairsList(String[] matchPairsList) {
                    setMatchPairsList(Js.<JsArray<String>>uncheckedCast(matchPairsList));
                }

                @JsProperty
                void setSpec(
                        RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType spec);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface CountFieldType {
                @JsOverlay
                static RollupRequest.ToObjectReturnType0.AggregationsListFieldType.CountFieldType create() {
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
                static RollupRequest.ToObjectReturnType0.AggregationsListFieldType.FirstRowKeyFieldType create() {
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
                static RollupRequest.ToObjectReturnType0.AggregationsListFieldType.PartitionFieldType create() {
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
            static RollupRequest.ToObjectReturnType0.AggregationsListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType getColumns();

            @JsProperty
            RollupRequest.ToObjectReturnType0.AggregationsListFieldType.CountFieldType getCount();

            @JsProperty
            RollupRequest.ToObjectReturnType0.AggregationsListFieldType.FirstRowKeyFieldType getFirstRowKey();

            @JsProperty
            Object getLastRowKey();

            @JsProperty
            RollupRequest.ToObjectReturnType0.AggregationsListFieldType.PartitionFieldType getPartition();

            @JsProperty
            void setColumns(
                    RollupRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType columns);

            @JsProperty
            void setCount(
                    RollupRequest.ToObjectReturnType0.AggregationsListFieldType.CountFieldType count);

            @JsProperty
            void setFirstRowKey(
                    RollupRequest.ToObjectReturnType0.AggregationsListFieldType.FirstRowKeyFieldType firstRowKey);

            @JsProperty
            void setLastRowKey(Object lastRowKey);

            @JsProperty
            void setPartition(
                    RollupRequest.ToObjectReturnType0.AggregationsListFieldType.PartitionFieldType partition);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultRollupTableIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static RollupRequest.ToObjectReturnType0.ResultRollupTableIdFieldType.GetTicketUnionType of(
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
            static RollupRequest.ToObjectReturnType0.ResultRollupTableIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            RollupRequest.ToObjectReturnType0.ResultRollupTableIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    RollupRequest.ToObjectReturnType0.ResultRollupTableIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<RollupRequest.ToObjectReturnType0.ResultRollupTableIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<RollupRequest.ToObjectReturnType0.ResultRollupTableIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsOverlay
        static RollupRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<RollupRequest.ToObjectReturnType0.AggregationsListFieldType> getAggregationsList();

        @JsProperty
        JsArray<String> getGroupByColumnsList();

        @JsProperty
        RollupRequest.ToObjectReturnType0.ResultRollupTableIdFieldType getResultRollupTableId();

        @JsProperty
        Object getSourceTableId();

        @JsProperty
        boolean isIncludeConstituents();

        @JsOverlay
        default void setAggregationsList(
                RollupRequest.ToObjectReturnType0.AggregationsListFieldType[] aggregationsList) {
            setAggregationsList(
                    Js.<JsArray<RollupRequest.ToObjectReturnType0.AggregationsListFieldType>>uncheckedCast(
                            aggregationsList));
        }

        @JsProperty
        void setAggregationsList(
                JsArray<RollupRequest.ToObjectReturnType0.AggregationsListFieldType> aggregationsList);

        @JsProperty
        void setGroupByColumnsList(JsArray<String> groupByColumnsList);

        @JsOverlay
        default void setGroupByColumnsList(String[] groupByColumnsList) {
            setGroupByColumnsList(Js.<JsArray<String>>uncheckedCast(groupByColumnsList));
        }

        @JsProperty
        void setIncludeConstituents(boolean includeConstituents);

        @JsProperty
        void setResultRollupTableId(
                RollupRequest.ToObjectReturnType0.ResultRollupTableIdFieldType resultRollupTableId);

        @JsProperty
        void setSourceTableId(Object sourceTableId);
    }

    public static native RollupRequest deserializeBinary(Uint8Array bytes);

    public static native RollupRequest deserializeBinaryFromReader(
            RollupRequest message, Object reader);

    public static native void serializeBinaryToWriter(RollupRequest message, Object writer);

    public static native RollupRequest.ToObjectReturnType toObject(
            boolean includeInstance, RollupRequest msg);

    public native Aggregation addAggregations();

    public native Aggregation addAggregations(Aggregation value, double index);

    public native Aggregation addAggregations(Aggregation value);

    public native String addGroupByColumns(String value, double index);

    public native String addGroupByColumns(String value);

    public native void clearAggregationsList();

    public native void clearGroupByColumnsList();

    public native void clearResultRollupTableId();

    public native void clearSourceTableId();

    public native JsArray<Aggregation> getAggregationsList();

    public native JsArray<String> getGroupByColumnsList();

    public native boolean getIncludeConstituents();

    public native Ticket getResultRollupTableId();

    public native Ticket getSourceTableId();

    public native boolean hasResultRollupTableId();

    public native boolean hasSourceTableId();

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

    public native void setIncludeConstituents(boolean value);

    public native void setResultRollupTableId();

    public native void setResultRollupTableId(Ticket value);

    public native void setSourceTableId();

    public native void setSourceTableId(Ticket value);

    public native RollupRequest.ToObjectReturnType0 toObject();

    public native RollupRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
