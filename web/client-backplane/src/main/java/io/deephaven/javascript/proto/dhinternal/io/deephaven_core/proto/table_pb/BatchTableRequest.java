//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.batchtablerequest.Operation;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.table_pb.BatchTableRequest",
        namespace = JsPackage.GLOBAL)
public class BatchTableRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface OpsListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface AggregateAllFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface SpecFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface ApproximatePercentileFieldType {
                        @JsOverlay
                        static BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType.SpecFieldType.ApproximatePercentileFieldType create() {
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
                        static BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType.SpecFieldType.CountDistinctFieldType create() {
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
                        static BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType.SpecFieldType.DistinctFieldType create() {
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
                        static BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType.SpecFieldType.FormulaFieldType create() {
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
                        static BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType.SpecFieldType.MedianFieldType create() {
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
                        static BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType.SpecFieldType.PercentileFieldType create() {
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
                            static BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType create() {
                                return Js.uncheckedCast(JsPropertyMap.of());
                            }

                            @JsProperty
                            String getColumnName();

                            @JsProperty
                            void setColumnName(String columnName);
                        }

                        @JsOverlay
                        static BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType.SpecFieldType.SortedFirstFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        JsArray<BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType> getColumnsList();

                        @JsOverlay
                        default void setColumnsList(
                                BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType[] columnsList) {
                            setColumnsList(
                                    Js.<JsArray<BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType>>uncheckedCast(
                                            columnsList));
                        }

                        @JsProperty
                        void setColumnsList(
                                JsArray<BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType> columnsList);
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface TDigestFieldType {
                        @JsOverlay
                        static BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType.SpecFieldType.TDigestFieldType create() {
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
                            static BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType create() {
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
                        static BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType.SpecFieldType.UniqueFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType getNonUniqueSentinel();

                        @JsProperty
                        boolean isIncludeNulls();

                        @JsProperty
                        void setIncludeNulls(boolean includeNulls);

                        @JsProperty
                        void setNonUniqueSentinel(
                                BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType nonUniqueSentinel);
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface WeightedAvgFieldType {
                        @JsOverlay
                        static BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType.SpecFieldType.WeightedAvgFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        String getWeightColumn();

                        @JsProperty
                        void setWeightColumn(String weightColumn);
                    }

                    @JsOverlay
                    static BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType.SpecFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    Object getAbsSum();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType.SpecFieldType.ApproximatePercentileFieldType getApproximatePercentile();

                    @JsProperty
                    Object getAvg();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType.SpecFieldType.CountDistinctFieldType getCountDistinct();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType.SpecFieldType.DistinctFieldType getDistinct();

                    @JsProperty
                    Object getFirst();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType.SpecFieldType.FormulaFieldType getFormula();

                    @JsProperty
                    Object getFreeze();

                    @JsProperty
                    Object getGroup();

                    @JsProperty
                    Object getLast();

                    @JsProperty
                    Object getMax();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType.SpecFieldType.MedianFieldType getMedian();

                    @JsProperty
                    Object getMin();

                    @JsProperty
                    Object getPb_var();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType.SpecFieldType.PercentileFieldType getPercentile();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType.SpecFieldType.SortedFirstFieldType getSortedFirst();

                    @JsProperty
                    Object getSortedLast();

                    @JsProperty
                    Object getStd();

                    @JsProperty
                    Object getSum();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType.SpecFieldType.TDigestFieldType getTDigest();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType.SpecFieldType.UniqueFieldType getUnique();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType.SpecFieldType.WeightedAvgFieldType getWeightedAvg();

                    @JsProperty
                    Object getWeightedSum();

                    @JsProperty
                    void setAbsSum(Object absSum);

                    @JsProperty
                    void setApproximatePercentile(
                            BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType.SpecFieldType.ApproximatePercentileFieldType approximatePercentile);

                    @JsProperty
                    void setAvg(Object avg);

                    @JsProperty
                    void setCountDistinct(
                            BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType.SpecFieldType.CountDistinctFieldType countDistinct);

                    @JsProperty
                    void setDistinct(
                            BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType.SpecFieldType.DistinctFieldType distinct);

                    @JsProperty
                    void setFirst(Object first);

                    @JsProperty
                    void setFormula(
                            BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType.SpecFieldType.FormulaFieldType formula);

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
                            BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType.SpecFieldType.MedianFieldType median);

                    @JsProperty
                    void setMin(Object min);

                    @JsProperty
                    void setPb_var(Object pb_var);

                    @JsProperty
                    void setPercentile(
                            BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType.SpecFieldType.PercentileFieldType percentile);

                    @JsProperty
                    void setSortedFirst(
                            BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType.SpecFieldType.SortedFirstFieldType sortedFirst);

                    @JsProperty
                    void setSortedLast(Object sortedLast);

                    @JsProperty
                    void setStd(Object std);

                    @JsProperty
                    void setSum(Object sum);

                    @JsProperty
                    void setTDigest(
                            BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType.SpecFieldType.TDigestFieldType tDigest);

                    @JsProperty
                    void setUnique(
                            BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType.SpecFieldType.UniqueFieldType unique);

                    @JsProperty
                    void setWeightedAvg(
                            BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType.SpecFieldType.WeightedAvgFieldType weightedAvg);

                    @JsProperty
                    void setWeightedSum(Object weightedSum);
                }

                @JsOverlay
                static BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<String> getGroupByColumnsList();

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getSourceId();

                @JsProperty
                BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType.SpecFieldType getSpec();

                @JsProperty
                void setGroupByColumnsList(JsArray<String> groupByColumnsList);

                @JsOverlay
                default void setGroupByColumnsList(String[] groupByColumnsList) {
                    setGroupByColumnsList(Js.<JsArray<String>>uncheckedCast(groupByColumnsList));
                }

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setSourceId(Object sourceId);

                @JsProperty
                void setSpec(
                        BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType.SpecFieldType spec);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface AggregateFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface AggregationsListFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface ColumnsFieldType {
                        @JsOverlay
                        static BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateFieldType.AggregationsListFieldType.ColumnsFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        JsArray<String> getMatchPairsList();

                        @JsProperty
                        Object getSpec();

                        @JsProperty
                        void setMatchPairsList(JsArray<String> matchPairsList);

                        @JsOverlay
                        default void setMatchPairsList(String[] matchPairsList) {
                            setMatchPairsList(Js.<JsArray<String>>uncheckedCast(matchPairsList));
                        }

                        @JsProperty
                        void setSpec(Object spec);
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface CountFieldType {
                        @JsOverlay
                        static BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateFieldType.AggregationsListFieldType.CountFieldType create() {
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
                        static BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateFieldType.AggregationsListFieldType.FirstRowKeyFieldType create() {
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
                        static BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateFieldType.AggregationsListFieldType.PartitionFieldType create() {
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
                    static BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateFieldType.AggregationsListFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateFieldType.AggregationsListFieldType.ColumnsFieldType getColumns();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateFieldType.AggregationsListFieldType.CountFieldType getCount();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateFieldType.AggregationsListFieldType.FirstRowKeyFieldType getFirstRowKey();

                    @JsProperty
                    Object getLastRowKey();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateFieldType.AggregationsListFieldType.PartitionFieldType getPartition();

                    @JsProperty
                    void setColumns(
                            BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateFieldType.AggregationsListFieldType.ColumnsFieldType columns);

                    @JsProperty
                    void setCount(
                            BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateFieldType.AggregationsListFieldType.CountFieldType count);

                    @JsProperty
                    void setFirstRowKey(
                            BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateFieldType.AggregationsListFieldType.FirstRowKeyFieldType firstRowKey);

                    @JsProperty
                    void setLastRowKey(Object lastRowKey);

                    @JsProperty
                    void setPartition(
                            BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateFieldType.AggregationsListFieldType.PartitionFieldType partition);
                }

                @JsOverlay
                static BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateFieldType.AggregationsListFieldType> getAggregationsList();

                @JsProperty
                JsArray<String> getGroupByColumnsList();

                @JsProperty
                Object getInitialGroupsId();

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getSourceId();

                @JsProperty
                boolean isPreserveEmpty();

                @JsOverlay
                default void setAggregationsList(
                        BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateFieldType.AggregationsListFieldType[] aggregationsList) {
                    setAggregationsList(
                            Js.<JsArray<BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateFieldType.AggregationsListFieldType>>uncheckedCast(
                                    aggregationsList));
                }

                @JsProperty
                void setAggregationsList(
                        JsArray<BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateFieldType.AggregationsListFieldType> aggregationsList);

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
                void setResultId(Object resultId);

                @JsProperty
                void setSourceId(Object sourceId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ApplyPreviewColumnsFieldType {
                @JsOverlay
                static BatchTableRequest.ToObjectReturnType.OpsListFieldType.ApplyPreviewColumnsFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getSourceId();

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setSourceId(Object sourceId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface AsOfJoinFieldType {
                @JsOverlay
                static BatchTableRequest.ToObjectReturnType.OpsListFieldType.AsOfJoinFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                double getAsOfMatchRule();

                @JsProperty
                JsArray<String> getColumnsToAddList();

                @JsProperty
                JsArray<String> getColumnsToMatchList();

                @JsProperty
                Object getLeftId();

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getRightId();

                @JsProperty
                void setAsOfMatchRule(double asOfMatchRule);

                @JsProperty
                void setColumnsToAddList(JsArray<String> columnsToAddList);

                @JsOverlay
                default void setColumnsToAddList(String[] columnsToAddList) {
                    setColumnsToAddList(Js.<JsArray<String>>uncheckedCast(columnsToAddList));
                }

                @JsProperty
                void setColumnsToMatchList(JsArray<String> columnsToMatchList);

                @JsOverlay
                default void setColumnsToMatchList(String[] columnsToMatchList) {
                    setColumnsToMatchList(Js.<JsArray<String>>uncheckedCast(columnsToMatchList));
                }

                @JsProperty
                void setLeftId(Object leftId);

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setRightId(Object rightId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ComboAggregateFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface AggregatesListFieldType {
                    @JsOverlay
                    static BatchTableRequest.ToObjectReturnType.OpsListFieldType.ComboAggregateFieldType.AggregatesListFieldType create() {
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

                @JsOverlay
                static BatchTableRequest.ToObjectReturnType.OpsListFieldType.ComboAggregateFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<BatchTableRequest.ToObjectReturnType.OpsListFieldType.ComboAggregateFieldType.AggregatesListFieldType> getAggregatesList();

                @JsProperty
                JsArray<String> getGroupByColumnsList();

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getSourceId();

                @JsProperty
                boolean isForceCombo();

                @JsOverlay
                default void setAggregatesList(
                        BatchTableRequest.ToObjectReturnType.OpsListFieldType.ComboAggregateFieldType.AggregatesListFieldType[] aggregatesList) {
                    setAggregatesList(
                            Js.<JsArray<BatchTableRequest.ToObjectReturnType.OpsListFieldType.ComboAggregateFieldType.AggregatesListFieldType>>uncheckedCast(
                                    aggregatesList));
                }

                @JsProperty
                void setAggregatesList(
                        JsArray<BatchTableRequest.ToObjectReturnType.OpsListFieldType.ComboAggregateFieldType.AggregatesListFieldType> aggregatesList);

                @JsProperty
                void setForceCombo(boolean forceCombo);

                @JsProperty
                void setGroupByColumnsList(JsArray<String> groupByColumnsList);

                @JsOverlay
                default void setGroupByColumnsList(String[] groupByColumnsList) {
                    setGroupByColumnsList(Js.<JsArray<String>>uncheckedCast(groupByColumnsList));
                }

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setSourceId(Object sourceId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface CreateInputTableFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface GetSchemaUnionType {
                    @JsOverlay
                    static BatchTableRequest.ToObjectReturnType.OpsListFieldType.CreateInputTableFieldType.GetSchemaUnionType of(
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

                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface KindFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface InMemoryKeyBackedFieldType {
                        @JsOverlay
                        static BatchTableRequest.ToObjectReturnType.OpsListFieldType.CreateInputTableFieldType.KindFieldType.InMemoryKeyBackedFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        JsArray<String> getKeyColumnsList();

                        @JsProperty
                        void setKeyColumnsList(JsArray<String> keyColumnsList);

                        @JsOverlay
                        default void setKeyColumnsList(String[] keyColumnsList) {
                            setKeyColumnsList(Js.<JsArray<String>>uncheckedCast(keyColumnsList));
                        }
                    }

                    @JsOverlay
                    static BatchTableRequest.ToObjectReturnType.OpsListFieldType.CreateInputTableFieldType.KindFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    Object getInMemoryAppendOnly();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.CreateInputTableFieldType.KindFieldType.InMemoryKeyBackedFieldType getInMemoryKeyBacked();

                    @JsProperty
                    void setInMemoryAppendOnly(Object inMemoryAppendOnly);

                    @JsProperty
                    void setInMemoryKeyBacked(
                            BatchTableRequest.ToObjectReturnType.OpsListFieldType.CreateInputTableFieldType.KindFieldType.InMemoryKeyBackedFieldType inMemoryKeyBacked);
                }

                @JsOverlay
                static BatchTableRequest.ToObjectReturnType.OpsListFieldType.CreateInputTableFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                BatchTableRequest.ToObjectReturnType.OpsListFieldType.CreateInputTableFieldType.KindFieldType getKind();

                @JsProperty
                Object getResultId();

                @JsProperty
                BatchTableRequest.ToObjectReturnType.OpsListFieldType.CreateInputTableFieldType.GetSchemaUnionType getSchema();

                @JsProperty
                Object getSourceTableId();

                @JsProperty
                void setKind(
                        BatchTableRequest.ToObjectReturnType.OpsListFieldType.CreateInputTableFieldType.KindFieldType kind);

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setSchema(
                        BatchTableRequest.ToObjectReturnType.OpsListFieldType.CreateInputTableFieldType.GetSchemaUnionType schema);

                @JsOverlay
                default void setSchema(String schema) {
                    setSchema(
                            Js.<BatchTableRequest.ToObjectReturnType.OpsListFieldType.CreateInputTableFieldType.GetSchemaUnionType>uncheckedCast(
                                    schema));
                }

                @JsOverlay
                default void setSchema(Uint8Array schema) {
                    setSchema(
                            Js.<BatchTableRequest.ToObjectReturnType.OpsListFieldType.CreateInputTableFieldType.GetSchemaUnionType>uncheckedCast(
                                    schema));
                }

                @JsProperty
                void setSourceTableId(Object sourceTableId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface CrossJoinFieldType {
                @JsOverlay
                static BatchTableRequest.ToObjectReturnType.OpsListFieldType.CrossJoinFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<String> getColumnsToAddList();

                @JsProperty
                JsArray<String> getColumnsToMatchList();

                @JsProperty
                Object getLeftId();

                @JsProperty
                double getReserveBits();

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getRightId();

                @JsProperty
                void setColumnsToAddList(JsArray<String> columnsToAddList);

                @JsOverlay
                default void setColumnsToAddList(String[] columnsToAddList) {
                    setColumnsToAddList(Js.<JsArray<String>>uncheckedCast(columnsToAddList));
                }

                @JsProperty
                void setColumnsToMatchList(JsArray<String> columnsToMatchList);

                @JsOverlay
                default void setColumnsToMatchList(String[] columnsToMatchList) {
                    setColumnsToMatchList(Js.<JsArray<String>>uncheckedCast(columnsToMatchList));
                }

                @JsProperty
                void setLeftId(Object leftId);

                @JsProperty
                void setReserveBits(double reserveBits);

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setRightId(Object rightId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface DropColumnsFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface SourceIdFieldType {
                    @JsOverlay
                    static BatchTableRequest.ToObjectReturnType.OpsListFieldType.DropColumnsFieldType.SourceIdFieldType create() {
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
                static BatchTableRequest.ToObjectReturnType.OpsListFieldType.DropColumnsFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<String> getColumnNamesList();

                @JsProperty
                Object getResultId();

                @JsProperty
                BatchTableRequest.ToObjectReturnType.OpsListFieldType.DropColumnsFieldType.SourceIdFieldType getSourceId();

                @JsProperty
                void setColumnNamesList(JsArray<String> columnNamesList);

                @JsOverlay
                default void setColumnNamesList(String[] columnNamesList) {
                    setColumnNamesList(Js.<JsArray<String>>uncheckedCast(columnNamesList));
                }

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setSourceId(
                        BatchTableRequest.ToObjectReturnType.OpsListFieldType.DropColumnsFieldType.SourceIdFieldType sourceId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface EmptyTableFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface ResultIdFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface GetTicketUnionType {
                        @JsOverlay
                        static BatchTableRequest.ToObjectReturnType.OpsListFieldType.EmptyTableFieldType.ResultIdFieldType.GetTicketUnionType of(
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
                    static BatchTableRequest.ToObjectReturnType.OpsListFieldType.EmptyTableFieldType.ResultIdFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.EmptyTableFieldType.ResultIdFieldType.GetTicketUnionType getTicket();

                    @JsProperty
                    void setTicket(
                            BatchTableRequest.ToObjectReturnType.OpsListFieldType.EmptyTableFieldType.ResultIdFieldType.GetTicketUnionType ticket);

                    @JsOverlay
                    default void setTicket(String ticket) {
                        setTicket(
                                Js.<BatchTableRequest.ToObjectReturnType.OpsListFieldType.EmptyTableFieldType.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                        ticket));
                    }

                    @JsOverlay
                    default void setTicket(Uint8Array ticket) {
                        setTicket(
                                Js.<BatchTableRequest.ToObjectReturnType.OpsListFieldType.EmptyTableFieldType.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                        ticket));
                    }
                }

                @JsOverlay
                static BatchTableRequest.ToObjectReturnType.OpsListFieldType.EmptyTableFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                BatchTableRequest.ToObjectReturnType.OpsListFieldType.EmptyTableFieldType.ResultIdFieldType getResultId();

                @JsProperty
                String getSize();

                @JsProperty
                void setResultId(
                        BatchTableRequest.ToObjectReturnType.OpsListFieldType.EmptyTableFieldType.ResultIdFieldType resultId);

                @JsProperty
                void setSize(String size);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ExactJoinFieldType {
                @JsOverlay
                static BatchTableRequest.ToObjectReturnType.OpsListFieldType.ExactJoinFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<String> getColumnsToAddList();

                @JsProperty
                JsArray<String> getColumnsToMatchList();

                @JsProperty
                Object getLeftId();

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getRightId();

                @JsProperty
                void setColumnsToAddList(JsArray<String> columnsToAddList);

                @JsOverlay
                default void setColumnsToAddList(String[] columnsToAddList) {
                    setColumnsToAddList(Js.<JsArray<String>>uncheckedCast(columnsToAddList));
                }

                @JsProperty
                void setColumnsToMatchList(JsArray<String> columnsToMatchList);

                @JsOverlay
                default void setColumnsToMatchList(String[] columnsToMatchList) {
                    setColumnsToMatchList(Js.<JsArray<String>>uncheckedCast(columnsToMatchList));
                }

                @JsProperty
                void setLeftId(Object leftId);

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setRightId(Object rightId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface FetchTableFieldType {
                @JsOverlay
                static BatchTableRequest.ToObjectReturnType.OpsListFieldType.FetchTableFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getSourceId();

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setSourceId(Object sourceId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface FilterFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface FiltersListFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface AndFieldType {
                        @JsOverlay
                        static BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType.AndFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        JsArray<Object> getFiltersList();

                        @JsProperty
                        void setFiltersList(JsArray<Object> filtersList);

                        @JsOverlay
                        default void setFiltersList(Object[] filtersList) {
                            setFiltersList(Js.<JsArray<Object>>uncheckedCast(filtersList));
                        }
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface CompareFieldType {
                        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                        public interface LhsFieldType {
                            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                            public interface LiteralFieldType {
                                @JsOverlay
                                static BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType.CompareFieldType.LhsFieldType.LiteralFieldType create() {
                                    return Js.uncheckedCast(JsPropertyMap.of());
                                }

                                @JsProperty
                                double getDoubleValue();

                                @JsProperty
                                String getLongValue();

                                @JsProperty
                                String getNanoTimeValue();

                                @JsProperty
                                String getStringValue();

                                @JsProperty
                                boolean isBoolValue();

                                @JsProperty
                                void setBoolValue(boolean boolValue);

                                @JsProperty
                                void setDoubleValue(double doubleValue);

                                @JsProperty
                                void setLongValue(String longValue);

                                @JsProperty
                                void setNanoTimeValue(String nanoTimeValue);

                                @JsProperty
                                void setStringValue(String stringValue);
                            }

                            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                            public interface ReferenceFieldType {
                                @JsOverlay
                                static BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType.CompareFieldType.LhsFieldType.ReferenceFieldType create() {
                                    return Js.uncheckedCast(JsPropertyMap.of());
                                }

                                @JsProperty
                                String getColumnName();

                                @JsProperty
                                void setColumnName(String columnName);
                            }

                            @JsOverlay
                            static BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType.CompareFieldType.LhsFieldType create() {
                                return Js.uncheckedCast(JsPropertyMap.of());
                            }

                            @JsProperty
                            BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType.CompareFieldType.LhsFieldType.LiteralFieldType getLiteral();

                            @JsProperty
                            BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType.CompareFieldType.LhsFieldType.ReferenceFieldType getReference();

                            @JsProperty
                            void setLiteral(
                                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType.CompareFieldType.LhsFieldType.LiteralFieldType literal);

                            @JsProperty
                            void setReference(
                                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType.CompareFieldType.LhsFieldType.ReferenceFieldType reference);
                        }

                        @JsOverlay
                        static BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType.CompareFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        double getCaseSensitivity();

                        @JsProperty
                        BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType.CompareFieldType.LhsFieldType getLhs();

                        @JsProperty
                        double getOperation();

                        @JsProperty
                        Object getRhs();

                        @JsProperty
                        void setCaseSensitivity(double caseSensitivity);

                        @JsProperty
                        void setLhs(
                                BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType.CompareFieldType.LhsFieldType lhs);

                        @JsProperty
                        void setOperation(double operation);

                        @JsProperty
                        void setRhs(Object rhs);
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface ContainsFieldType {
                        @JsOverlay
                        static BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType.ContainsFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        double getCaseSensitivity();

                        @JsProperty
                        double getMatchType();

                        @JsProperty
                        Object getReference();

                        @JsProperty
                        String getSearchString();

                        @JsProperty
                        void setCaseSensitivity(double caseSensitivity);

                        @JsProperty
                        void setMatchType(double matchType);

                        @JsProperty
                        void setReference(Object reference);

                        @JsProperty
                        void setSearchString(String searchString);
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface InvokeFieldType {
                        @JsOverlay
                        static BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType.InvokeFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        JsArray<Object> getArgumentsList();

                        @JsProperty
                        String getMethod();

                        @JsProperty
                        Object getTarget();

                        @JsProperty
                        void setArgumentsList(JsArray<Object> argumentsList);

                        @JsOverlay
                        default void setArgumentsList(Object[] argumentsList) {
                            setArgumentsList(Js.<JsArray<Object>>uncheckedCast(argumentsList));
                        }

                        @JsProperty
                        void setMethod(String method);

                        @JsProperty
                        void setTarget(Object target);
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface IsNullFieldType {
                        @JsOverlay
                        static BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType.IsNullFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        Object getReference();

                        @JsProperty
                        void setReference(Object reference);
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface MatchesFieldType {
                        @JsOverlay
                        static BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType.MatchesFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        double getCaseSensitivity();

                        @JsProperty
                        double getMatchType();

                        @JsProperty
                        Object getReference();

                        @JsProperty
                        String getRegex();

                        @JsProperty
                        void setCaseSensitivity(double caseSensitivity);

                        @JsProperty
                        void setMatchType(double matchType);

                        @JsProperty
                        void setReference(Object reference);

                        @JsProperty
                        void setRegex(String regex);
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface NotFieldType {
                        @JsOverlay
                        static BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType.NotFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        Object getFilter();

                        @JsProperty
                        void setFilter(Object filter);
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface OrFieldType {
                        @JsOverlay
                        static BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType.OrFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        JsArray<Object> getFiltersList();

                        @JsProperty
                        void setFiltersList(JsArray<Object> filtersList);

                        @JsOverlay
                        default void setFiltersList(Object[] filtersList) {
                            setFiltersList(Js.<JsArray<Object>>uncheckedCast(filtersList));
                        }
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface Pb_inFieldType {
                        @JsOverlay
                        static BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType.Pb_inFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        JsArray<Object> getCandidatesList();

                        @JsProperty
                        double getCaseSensitivity();

                        @JsProperty
                        double getMatchType();

                        @JsProperty
                        Object getTarget();

                        @JsProperty
                        void setCandidatesList(JsArray<Object> candidatesList);

                        @JsOverlay
                        default void setCandidatesList(Object[] candidatesList) {
                            setCandidatesList(Js.<JsArray<Object>>uncheckedCast(candidatesList));
                        }

                        @JsProperty
                        void setCaseSensitivity(double caseSensitivity);

                        @JsProperty
                        void setMatchType(double matchType);

                        @JsProperty
                        void setTarget(Object target);
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface SearchFieldType {
                        @JsOverlay
                        static BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType.SearchFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        JsArray<Object> getOptionalReferencesList();

                        @JsProperty
                        String getSearchString();

                        @JsProperty
                        void setOptionalReferencesList(JsArray<Object> optionalReferencesList);

                        @JsOverlay
                        default void setOptionalReferencesList(Object[] optionalReferencesList) {
                            setOptionalReferencesList(Js.<JsArray<Object>>uncheckedCast(optionalReferencesList));
                        }

                        @JsProperty
                        void setSearchString(String searchString);
                    }

                    @JsOverlay
                    static BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType.AndFieldType getAnd();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType.CompareFieldType getCompare();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType.ContainsFieldType getContains();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType.InvokeFieldType getInvoke();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType.IsNullFieldType getIsNull();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType.MatchesFieldType getMatches();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType.NotFieldType getNot();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType.OrFieldType getOr();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType.Pb_inFieldType getPb_in();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType.SearchFieldType getSearch();

                    @JsProperty
                    void setAnd(
                            BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType.AndFieldType and);

                    @JsProperty
                    void setCompare(
                            BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType.CompareFieldType compare);

                    @JsProperty
                    void setContains(
                            BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType.ContainsFieldType contains);

                    @JsProperty
                    void setInvoke(
                            BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType.InvokeFieldType invoke);

                    @JsProperty
                    void setIsNull(
                            BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType.IsNullFieldType isNull);

                    @JsProperty
                    void setMatches(
                            BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType.MatchesFieldType matches);

                    @JsProperty
                    void setNot(
                            BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType.NotFieldType not);

                    @JsProperty
                    void setOr(
                            BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType.OrFieldType or);

                    @JsProperty
                    void setPb_in(
                            BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType.Pb_inFieldType pb_in);

                    @JsProperty
                    void setSearch(
                            BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType.SearchFieldType search);
                }

                @JsOverlay
                static BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType> getFiltersList();

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getSourceId();

                @JsOverlay
                default void setFiltersList(
                        BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType[] filtersList) {
                    setFiltersList(
                            Js.<JsArray<BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType>>uncheckedCast(
                                    filtersList));
                }

                @JsProperty
                void setFiltersList(
                        JsArray<BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType.FiltersListFieldType> filtersList);

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setSourceId(Object sourceId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface FlattenFieldType {
                @JsOverlay
                static BatchTableRequest.ToObjectReturnType.OpsListFieldType.FlattenFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getSourceId();

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setSourceId(Object sourceId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface HeadByFieldType {
                @JsOverlay
                static BatchTableRequest.ToObjectReturnType.OpsListFieldType.HeadByFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<String> getGroupByColumnSpecsList();

                @JsProperty
                String getNumRows();

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getSourceId();

                @JsProperty
                void setGroupByColumnSpecsList(JsArray<String> groupByColumnSpecsList);

                @JsOverlay
                default void setGroupByColumnSpecsList(String[] groupByColumnSpecsList) {
                    setGroupByColumnSpecsList(Js.<JsArray<String>>uncheckedCast(groupByColumnSpecsList));
                }

                @JsProperty
                void setNumRows(String numRows);

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setSourceId(Object sourceId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface HeadFieldType {
                @JsOverlay
                static BatchTableRequest.ToObjectReturnType.OpsListFieldType.HeadFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getNumRows();

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getSourceId();

                @JsProperty
                void setNumRows(String numRows);

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setSourceId(Object sourceId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface LeftJoinFieldType {
                @JsOverlay
                static BatchTableRequest.ToObjectReturnType.OpsListFieldType.LeftJoinFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<String> getColumnsToAddList();

                @JsProperty
                JsArray<String> getColumnsToMatchList();

                @JsProperty
                Object getLeftId();

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getRightId();

                @JsProperty
                void setColumnsToAddList(JsArray<String> columnsToAddList);

                @JsOverlay
                default void setColumnsToAddList(String[] columnsToAddList) {
                    setColumnsToAddList(Js.<JsArray<String>>uncheckedCast(columnsToAddList));
                }

                @JsProperty
                void setColumnsToMatchList(JsArray<String> columnsToMatchList);

                @JsOverlay
                default void setColumnsToMatchList(String[] columnsToMatchList) {
                    setColumnsToMatchList(Js.<JsArray<String>>uncheckedCast(columnsToMatchList));
                }

                @JsProperty
                void setLeftId(Object leftId);

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setRightId(Object rightId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface MergeFieldType {
                @JsOverlay
                static BatchTableRequest.ToObjectReturnType.OpsListFieldType.MergeFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getKeyColumn();

                @JsProperty
                Object getResultId();

                @JsProperty
                JsArray<Object> getSourceIdsList();

                @JsProperty
                void setKeyColumn(String keyColumn);

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setSourceIdsList(JsArray<Object> sourceIdsList);

                @JsOverlay
                default void setSourceIdsList(Object[] sourceIdsList) {
                    setSourceIdsList(Js.<JsArray<Object>>uncheckedCast(sourceIdsList));
                }
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface NaturalJoinFieldType {
                @JsOverlay
                static BatchTableRequest.ToObjectReturnType.OpsListFieldType.NaturalJoinFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<String> getColumnsToAddList();

                @JsProperty
                JsArray<String> getColumnsToMatchList();

                @JsProperty
                Object getLeftId();

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getRightId();

                @JsProperty
                void setColumnsToAddList(JsArray<String> columnsToAddList);

                @JsOverlay
                default void setColumnsToAddList(String[] columnsToAddList) {
                    setColumnsToAddList(Js.<JsArray<String>>uncheckedCast(columnsToAddList));
                }

                @JsProperty
                void setColumnsToMatchList(JsArray<String> columnsToMatchList);

                @JsOverlay
                default void setColumnsToMatchList(String[] columnsToMatchList) {
                    setColumnsToMatchList(Js.<JsArray<String>>uncheckedCast(columnsToMatchList));
                }

                @JsProperty
                void setLeftId(Object leftId);

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setRightId(Object rightId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface RunChartDownsampleFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface ZoomRangeFieldType {
                    @JsOverlay
                    static BatchTableRequest.ToObjectReturnType.OpsListFieldType.RunChartDownsampleFieldType.ZoomRangeFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    String getMaxDateNanos();

                    @JsProperty
                    String getMinDateNanos();

                    @JsProperty
                    void setMaxDateNanos(String maxDateNanos);

                    @JsProperty
                    void setMinDateNanos(String minDateNanos);
                }

                @JsOverlay
                static BatchTableRequest.ToObjectReturnType.OpsListFieldType.RunChartDownsampleFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                double getPixelCount();

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getSourceId();

                @JsProperty
                String getXColumnName();

                @JsProperty
                JsArray<String> getYColumnNamesList();

                @JsProperty
                BatchTableRequest.ToObjectReturnType.OpsListFieldType.RunChartDownsampleFieldType.ZoomRangeFieldType getZoomRange();

                @JsProperty
                void setPixelCount(double pixelCount);

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setSourceId(Object sourceId);

                @JsProperty
                void setXColumnName(String xColumnName);

                @JsProperty
                void setYColumnNamesList(JsArray<String> yColumnNamesList);

                @JsOverlay
                default void setYColumnNamesList(String[] yColumnNamesList) {
                    setYColumnNamesList(Js.<JsArray<String>>uncheckedCast(yColumnNamesList));
                }

                @JsProperty
                void setZoomRange(
                        BatchTableRequest.ToObjectReturnType.OpsListFieldType.RunChartDownsampleFieldType.ZoomRangeFieldType zoomRange);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface SelectDistinctFieldType {
                @JsOverlay
                static BatchTableRequest.ToObjectReturnType.OpsListFieldType.SelectDistinctFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<String> getColumnNamesList();

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getSourceId();

                @JsProperty
                void setColumnNamesList(JsArray<String> columnNamesList);

                @JsOverlay
                default void setColumnNamesList(String[] columnNamesList) {
                    setColumnNamesList(Js.<JsArray<String>>uncheckedCast(columnNamesList));
                }

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setSourceId(Object sourceId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface SnapshotFieldType {
                @JsOverlay
                static BatchTableRequest.ToObjectReturnType.OpsListFieldType.SnapshotFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getSourceId();

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setSourceId(Object sourceId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface SnapshotWhenFieldType {
                @JsOverlay
                static BatchTableRequest.ToObjectReturnType.OpsListFieldType.SnapshotWhenFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getBaseId();

                @JsProperty
                Object getResultId();

                @JsProperty
                JsArray<String> getStampColumnsList();

                @JsProperty
                Object getTriggerId();

                @JsProperty
                boolean isHistory();

                @JsProperty
                boolean isIncremental();

                @JsProperty
                boolean isInitial();

                @JsProperty
                void setBaseId(Object baseId);

                @JsProperty
                void setHistory(boolean history);

                @JsProperty
                void setIncremental(boolean incremental);

                @JsProperty
                void setInitial(boolean initial);

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setStampColumnsList(JsArray<String> stampColumnsList);

                @JsOverlay
                default void setStampColumnsList(String[] stampColumnsList) {
                    setStampColumnsList(Js.<JsArray<String>>uncheckedCast(stampColumnsList));
                }

                @JsProperty
                void setTriggerId(Object triggerId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface SortFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface SortsListFieldType {
                    @JsOverlay
                    static BatchTableRequest.ToObjectReturnType.OpsListFieldType.SortFieldType.SortsListFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    String getColumnName();

                    @JsProperty
                    double getDirection();

                    @JsProperty
                    boolean isIsAbsolute();

                    @JsProperty
                    void setColumnName(String columnName);

                    @JsProperty
                    void setDirection(double direction);

                    @JsProperty
                    void setIsAbsolute(boolean isAbsolute);
                }

                @JsOverlay
                static BatchTableRequest.ToObjectReturnType.OpsListFieldType.SortFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getResultId();

                @JsProperty
                JsArray<BatchTableRequest.ToObjectReturnType.OpsListFieldType.SortFieldType.SortsListFieldType> getSortsList();

                @JsProperty
                Object getSourceId();

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setSortsList(
                        JsArray<BatchTableRequest.ToObjectReturnType.OpsListFieldType.SortFieldType.SortsListFieldType> sortsList);

                @JsOverlay
                default void setSortsList(
                        BatchTableRequest.ToObjectReturnType.OpsListFieldType.SortFieldType.SortsListFieldType[] sortsList) {
                    setSortsList(
                            Js.<JsArray<BatchTableRequest.ToObjectReturnType.OpsListFieldType.SortFieldType.SortsListFieldType>>uncheckedCast(
                                    sortsList));
                }

                @JsProperty
                void setSourceId(Object sourceId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TimeTableFieldType {
                @JsOverlay
                static BatchTableRequest.ToObjectReturnType.OpsListFieldType.TimeTableFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getPeriodNanos();

                @JsProperty
                Object getResultId();

                @JsProperty
                String getStartTimeNanos();

                @JsProperty
                void setPeriodNanos(String periodNanos);

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setStartTimeNanos(String startTimeNanos);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface UngroupFieldType {
                @JsOverlay
                static BatchTableRequest.ToObjectReturnType.OpsListFieldType.UngroupFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<String> getColumnsToUngroupList();

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getSourceId();

                @JsProperty
                boolean isNullFill();

                @JsProperty
                void setColumnsToUngroupList(JsArray<String> columnsToUngroupList);

                @JsOverlay
                default void setColumnsToUngroupList(String[] columnsToUngroupList) {
                    setColumnsToUngroupList(Js.<JsArray<String>>uncheckedCast(columnsToUngroupList));
                }

                @JsProperty
                void setNullFill(boolean nullFill);

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setSourceId(Object sourceId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface UnstructuredFilterFieldType {
                @JsOverlay
                static BatchTableRequest.ToObjectReturnType.OpsListFieldType.UnstructuredFilterFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<String> getFiltersList();

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getSourceId();

                @JsProperty
                void setFiltersList(JsArray<String> filtersList);

                @JsOverlay
                default void setFiltersList(String[] filtersList) {
                    setFiltersList(Js.<JsArray<String>>uncheckedCast(filtersList));
                }

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setSourceId(Object sourceId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface UpdateByFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface OperationsListFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface ColumnFieldType {
                        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                        public interface SpecFieldType {
                            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                            public interface EmaFieldType {
                                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                                public interface OptionsFieldType {
                                    @JsOverlay
                                    static BatchTableRequest.ToObjectReturnType.OpsListFieldType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.OptionsFieldType create() {
                                        return Js.uncheckedCast(JsPropertyMap.of());
                                    }

                                    @JsProperty
                                    Object getBigValueContext();

                                    @JsProperty
                                    double getOnNanValue();

                                    @JsProperty
                                    double getOnNegativeDeltaTime();

                                    @JsProperty
                                    double getOnNullTime();

                                    @JsProperty
                                    double getOnNullValue();

                                    @JsProperty
                                    double getOnZeroDeltaTime();

                                    @JsProperty
                                    void setBigValueContext(Object bigValueContext);

                                    @JsProperty
                                    void setOnNanValue(double onNanValue);

                                    @JsProperty
                                    void setOnNegativeDeltaTime(double onNegativeDeltaTime);

                                    @JsProperty
                                    void setOnNullTime(double onNullTime);

                                    @JsProperty
                                    void setOnNullValue(double onNullValue);

                                    @JsProperty
                                    void setOnZeroDeltaTime(double onZeroDeltaTime);
                                }

                                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                                public interface TimescaleFieldType {
                                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                                    public interface TicksFieldType {
                                        @JsOverlay
                                        static BatchTableRequest.ToObjectReturnType.OpsListFieldType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TicksFieldType create() {
                                            return Js.uncheckedCast(JsPropertyMap.of());
                                        }

                                        @JsProperty
                                        String getTicks();

                                        @JsProperty
                                        void setTicks(String ticks);
                                    }

                                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                                    public interface TimeFieldType {
                                        @JsOverlay
                                        static BatchTableRequest.ToObjectReturnType.OpsListFieldType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TimeFieldType create() {
                                            return Js.uncheckedCast(JsPropertyMap.of());
                                        }

                                        @JsProperty
                                        String getColumn();

                                        @JsProperty
                                        String getPeriodNanos();

                                        @JsProperty
                                        void setColumn(String column);

                                        @JsProperty
                                        void setPeriodNanos(String periodNanos);
                                    }

                                    @JsOverlay
                                    static BatchTableRequest.ToObjectReturnType.OpsListFieldType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType create() {
                                        return Js.uncheckedCast(JsPropertyMap.of());
                                    }

                                    @JsProperty
                                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TicksFieldType getTicks();

                                    @JsProperty
                                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TimeFieldType getTime();

                                    @JsProperty
                                    void setTicks(
                                            BatchTableRequest.ToObjectReturnType.OpsListFieldType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TicksFieldType ticks);

                                    @JsProperty
                                    void setTime(
                                            BatchTableRequest.ToObjectReturnType.OpsListFieldType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TimeFieldType time);
                                }

                                @JsOverlay
                                static BatchTableRequest.ToObjectReturnType.OpsListFieldType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType create() {
                                    return Js.uncheckedCast(JsPropertyMap.of());
                                }

                                @JsProperty
                                BatchTableRequest.ToObjectReturnType.OpsListFieldType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.OptionsFieldType getOptions();

                                @JsProperty
                                BatchTableRequest.ToObjectReturnType.OpsListFieldType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType getTimescale();

                                @JsProperty
                                void setOptions(
                                        BatchTableRequest.ToObjectReturnType.OpsListFieldType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.OptionsFieldType options);

                                @JsProperty
                                void setTimescale(
                                        BatchTableRequest.ToObjectReturnType.OpsListFieldType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType timescale);
                            }

                            @JsOverlay
                            static BatchTableRequest.ToObjectReturnType.OpsListFieldType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType create() {
                                return Js.uncheckedCast(JsPropertyMap.of());
                            }

                            @JsProperty
                            BatchTableRequest.ToObjectReturnType.OpsListFieldType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType getEma();

                            @JsProperty
                            Object getFill();

                            @JsProperty
                            Object getMax();

                            @JsProperty
                            Object getMin();

                            @JsProperty
                            Object getProduct();

                            @JsProperty
                            Object getSum();

                            @JsProperty
                            void setEma(
                                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType ema);

                            @JsProperty
                            void setFill(Object fill);

                            @JsProperty
                            void setMax(Object max);

                            @JsProperty
                            void setMin(Object min);

                            @JsProperty
                            void setProduct(Object product);

                            @JsProperty
                            void setSum(Object sum);
                        }

                        @JsOverlay
                        static BatchTableRequest.ToObjectReturnType.OpsListFieldType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        JsArray<String> getMatchPairsList();

                        @JsProperty
                        BatchTableRequest.ToObjectReturnType.OpsListFieldType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType getSpec();

                        @JsProperty
                        void setMatchPairsList(JsArray<String> matchPairsList);

                        @JsOverlay
                        default void setMatchPairsList(String[] matchPairsList) {
                            setMatchPairsList(Js.<JsArray<String>>uncheckedCast(matchPairsList));
                        }

                        @JsProperty
                        void setSpec(
                                BatchTableRequest.ToObjectReturnType.OpsListFieldType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType spec);
                    }

                    @JsOverlay
                    static BatchTableRequest.ToObjectReturnType.OpsListFieldType.UpdateByFieldType.OperationsListFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType getColumn();

                    @JsProperty
                    void setColumn(
                            BatchTableRequest.ToObjectReturnType.OpsListFieldType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType column);
                }

                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface OptionsFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface MathContextFieldType {
                        @JsOverlay
                        static BatchTableRequest.ToObjectReturnType.OpsListFieldType.UpdateByFieldType.OptionsFieldType.MathContextFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        double getPrecision();

                        @JsProperty
                        double getRoundingMode();

                        @JsProperty
                        void setPrecision(double precision);

                        @JsProperty
                        void setRoundingMode(double roundingMode);
                    }

                    @JsOverlay
                    static BatchTableRequest.ToObjectReturnType.OpsListFieldType.UpdateByFieldType.OptionsFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    double getChunkCapacity();

                    @JsProperty
                    double getInitialHashTableSize();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.UpdateByFieldType.OptionsFieldType.MathContextFieldType getMathContext();

                    @JsProperty
                    double getMaxStaticSparseMemoryOverhead();

                    @JsProperty
                    double getMaximumLoadFactor();

                    @JsProperty
                    double getTargetLoadFactor();

                    @JsProperty
                    boolean isUseRedirection();

                    @JsProperty
                    void setChunkCapacity(double chunkCapacity);

                    @JsProperty
                    void setInitialHashTableSize(double initialHashTableSize);

                    @JsProperty
                    void setMathContext(
                            BatchTableRequest.ToObjectReturnType.OpsListFieldType.UpdateByFieldType.OptionsFieldType.MathContextFieldType mathContext);

                    @JsProperty
                    void setMaxStaticSparseMemoryOverhead(double maxStaticSparseMemoryOverhead);

                    @JsProperty
                    void setMaximumLoadFactor(double maximumLoadFactor);

                    @JsProperty
                    void setTargetLoadFactor(double targetLoadFactor);

                    @JsProperty
                    void setUseRedirection(boolean useRedirection);
                }

                @JsOverlay
                static BatchTableRequest.ToObjectReturnType.OpsListFieldType.UpdateByFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<String> getGroupByColumnsList();

                @JsProperty
                JsArray<BatchTableRequest.ToObjectReturnType.OpsListFieldType.UpdateByFieldType.OperationsListFieldType> getOperationsList();

                @JsProperty
                BatchTableRequest.ToObjectReturnType.OpsListFieldType.UpdateByFieldType.OptionsFieldType getOptions();

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getSourceId();

                @JsProperty
                void setGroupByColumnsList(JsArray<String> groupByColumnsList);

                @JsOverlay
                default void setGroupByColumnsList(String[] groupByColumnsList) {
                    setGroupByColumnsList(Js.<JsArray<String>>uncheckedCast(groupByColumnsList));
                }

                @JsProperty
                void setOperationsList(
                        JsArray<BatchTableRequest.ToObjectReturnType.OpsListFieldType.UpdateByFieldType.OperationsListFieldType> operationsList);

                @JsOverlay
                default void setOperationsList(
                        BatchTableRequest.ToObjectReturnType.OpsListFieldType.UpdateByFieldType.OperationsListFieldType[] operationsList) {
                    setOperationsList(
                            Js.<JsArray<BatchTableRequest.ToObjectReturnType.OpsListFieldType.UpdateByFieldType.OperationsListFieldType>>uncheckedCast(
                                    operationsList));
                }

                @JsProperty
                void setOptions(
                        BatchTableRequest.ToObjectReturnType.OpsListFieldType.UpdateByFieldType.OptionsFieldType options);

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setSourceId(Object sourceId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface UpdateFieldType {
                @JsOverlay
                static BatchTableRequest.ToObjectReturnType.OpsListFieldType.UpdateFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<String> getColumnSpecsList();

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getSourceId();

                @JsProperty
                void setColumnSpecsList(JsArray<String> columnSpecsList);

                @JsOverlay
                default void setColumnSpecsList(String[] columnSpecsList) {
                    setColumnSpecsList(Js.<JsArray<String>>uncheckedCast(columnSpecsList));
                }

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setSourceId(Object sourceId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface WhereInFieldType {
                @JsOverlay
                static BatchTableRequest.ToObjectReturnType.OpsListFieldType.WhereInFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<String> getColumnsToMatchList();

                @JsProperty
                Object getLeftId();

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getRightId();

                @JsProperty
                boolean isInverted();

                @JsProperty
                void setColumnsToMatchList(JsArray<String> columnsToMatchList);

                @JsOverlay
                default void setColumnsToMatchList(String[] columnsToMatchList) {
                    setColumnsToMatchList(Js.<JsArray<String>>uncheckedCast(columnsToMatchList));
                }

                @JsProperty
                void setInverted(boolean inverted);

                @JsProperty
                void setLeftId(Object leftId);

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setRightId(Object rightId);
            }

            @JsOverlay
            static BatchTableRequest.ToObjectReturnType.OpsListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateFieldType getAggregate();

            @JsProperty
            BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType getAggregateAll();

            @JsProperty
            BatchTableRequest.ToObjectReturnType.OpsListFieldType.ApplyPreviewColumnsFieldType getApplyPreviewColumns();

            @JsProperty
            BatchTableRequest.ToObjectReturnType.OpsListFieldType.AsOfJoinFieldType getAsOfJoin();

            @JsProperty
            BatchTableRequest.ToObjectReturnType.OpsListFieldType.ComboAggregateFieldType getComboAggregate();

            @JsProperty
            BatchTableRequest.ToObjectReturnType.OpsListFieldType.CreateInputTableFieldType getCreateInputTable();

            @JsProperty
            BatchTableRequest.ToObjectReturnType.OpsListFieldType.CrossJoinFieldType getCrossJoin();

            @JsProperty
            BatchTableRequest.ToObjectReturnType.OpsListFieldType.DropColumnsFieldType getDropColumns();

            @JsProperty
            BatchTableRequest.ToObjectReturnType.OpsListFieldType.EmptyTableFieldType getEmptyTable();

            @JsProperty
            BatchTableRequest.ToObjectReturnType.OpsListFieldType.ExactJoinFieldType getExactJoin();

            @JsProperty
            BatchTableRequest.ToObjectReturnType.OpsListFieldType.FetchTableFieldType getFetchTable();

            @JsProperty
            BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType getFilter();

            @JsProperty
            BatchTableRequest.ToObjectReturnType.OpsListFieldType.FlattenFieldType getFlatten();

            @JsProperty
            BatchTableRequest.ToObjectReturnType.OpsListFieldType.HeadFieldType getHead();

            @JsProperty
            BatchTableRequest.ToObjectReturnType.OpsListFieldType.HeadByFieldType getHeadBy();

            @JsProperty
            Object getLazyUpdate();

            @JsProperty
            BatchTableRequest.ToObjectReturnType.OpsListFieldType.LeftJoinFieldType getLeftJoin();

            @JsProperty
            BatchTableRequest.ToObjectReturnType.OpsListFieldType.MergeFieldType getMerge();

            @JsProperty
            BatchTableRequest.ToObjectReturnType.OpsListFieldType.NaturalJoinFieldType getNaturalJoin();

            @JsProperty
            BatchTableRequest.ToObjectReturnType.OpsListFieldType.RunChartDownsampleFieldType getRunChartDownsample();

            @JsProperty
            Object getSelect();

            @JsProperty
            BatchTableRequest.ToObjectReturnType.OpsListFieldType.SelectDistinctFieldType getSelectDistinct();

            @JsProperty
            BatchTableRequest.ToObjectReturnType.OpsListFieldType.SnapshotFieldType getSnapshot();

            @JsProperty
            BatchTableRequest.ToObjectReturnType.OpsListFieldType.SnapshotWhenFieldType getSnapshotWhen();

            @JsProperty
            BatchTableRequest.ToObjectReturnType.OpsListFieldType.SortFieldType getSort();

            @JsProperty
            Object getTail();

            @JsProperty
            Object getTailBy();

            @JsProperty
            BatchTableRequest.ToObjectReturnType.OpsListFieldType.TimeTableFieldType getTimeTable();

            @JsProperty
            BatchTableRequest.ToObjectReturnType.OpsListFieldType.UngroupFieldType getUngroup();

            @JsProperty
            BatchTableRequest.ToObjectReturnType.OpsListFieldType.UnstructuredFilterFieldType getUnstructuredFilter();

            @JsProperty
            BatchTableRequest.ToObjectReturnType.OpsListFieldType.UpdateFieldType getUpdate();

            @JsProperty
            BatchTableRequest.ToObjectReturnType.OpsListFieldType.UpdateByFieldType getUpdateBy();

            @JsProperty
            Object getUpdateView();

            @JsProperty
            Object getView();

            @JsProperty
            BatchTableRequest.ToObjectReturnType.OpsListFieldType.WhereInFieldType getWhereIn();

            @JsProperty
            void setAggregate(
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateFieldType aggregate);

            @JsProperty
            void setAggregateAll(
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.AggregateAllFieldType aggregateAll);

            @JsProperty
            void setApplyPreviewColumns(
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.ApplyPreviewColumnsFieldType applyPreviewColumns);

            @JsProperty
            void setAsOfJoin(
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.AsOfJoinFieldType asOfJoin);

            @JsProperty
            void setComboAggregate(
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.ComboAggregateFieldType comboAggregate);

            @JsProperty
            void setCreateInputTable(
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.CreateInputTableFieldType createInputTable);

            @JsProperty
            void setCrossJoin(
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.CrossJoinFieldType crossJoin);

            @JsProperty
            void setDropColumns(
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.DropColumnsFieldType dropColumns);

            @JsProperty
            void setEmptyTable(
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.EmptyTableFieldType emptyTable);

            @JsProperty
            void setExactJoin(
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.ExactJoinFieldType exactJoin);

            @JsProperty
            void setFetchTable(
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.FetchTableFieldType fetchTable);

            @JsProperty
            void setFilter(BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType filter);

            @JsProperty
            void setFlatten(
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.FlattenFieldType flatten);

            @JsProperty
            void setHead(BatchTableRequest.ToObjectReturnType.OpsListFieldType.HeadFieldType head);

            @JsProperty
            void setHeadBy(BatchTableRequest.ToObjectReturnType.OpsListFieldType.HeadByFieldType headBy);

            @JsProperty
            void setLazyUpdate(Object lazyUpdate);

            @JsProperty
            void setLeftJoin(
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.LeftJoinFieldType leftJoin);

            @JsProperty
            void setMerge(BatchTableRequest.ToObjectReturnType.OpsListFieldType.MergeFieldType merge);

            @JsProperty
            void setNaturalJoin(
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.NaturalJoinFieldType naturalJoin);

            @JsProperty
            void setRunChartDownsample(
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.RunChartDownsampleFieldType runChartDownsample);

            @JsProperty
            void setSelect(Object select);

            @JsProperty
            void setSelectDistinct(
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.SelectDistinctFieldType selectDistinct);

            @JsProperty
            void setSnapshot(
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.SnapshotFieldType snapshot);

            @JsProperty
            void setSnapshotWhen(
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.SnapshotWhenFieldType snapshotWhen);

            @JsProperty
            void setSort(BatchTableRequest.ToObjectReturnType.OpsListFieldType.SortFieldType sort);

            @JsProperty
            void setTail(Object tail);

            @JsProperty
            void setTailBy(Object tailBy);

            @JsProperty
            void setTimeTable(
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.TimeTableFieldType timeTable);

            @JsProperty
            void setUngroup(
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.UngroupFieldType ungroup);

            @JsProperty
            void setUnstructuredFilter(
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.UnstructuredFilterFieldType unstructuredFilter);

            @JsProperty
            void setUpdate(BatchTableRequest.ToObjectReturnType.OpsListFieldType.UpdateFieldType update);

            @JsProperty
            void setUpdateBy(
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.UpdateByFieldType updateBy);

            @JsProperty
            void setUpdateView(Object updateView);

            @JsProperty
            void setView(Object view);

            @JsProperty
            void setWhereIn(
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.WhereInFieldType whereIn);
        }

        @JsOverlay
        static BatchTableRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<BatchTableRequest.ToObjectReturnType.OpsListFieldType> getOpsList();

        @JsProperty
        void setOpsList(JsArray<BatchTableRequest.ToObjectReturnType.OpsListFieldType> opsList);

        @JsOverlay
        default void setOpsList(BatchTableRequest.ToObjectReturnType.OpsListFieldType[] opsList) {
            setOpsList(
                    Js.<JsArray<BatchTableRequest.ToObjectReturnType.OpsListFieldType>>uncheckedCast(
                            opsList));
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface OpsListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface AggregateAllFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface SpecFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface ApproximatePercentileFieldType {
                        @JsOverlay
                        static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType.SpecFieldType.ApproximatePercentileFieldType create() {
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
                        static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType.SpecFieldType.CountDistinctFieldType create() {
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
                        static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType.SpecFieldType.DistinctFieldType create() {
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
                        static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType.SpecFieldType.FormulaFieldType create() {
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
                        static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType.SpecFieldType.MedianFieldType create() {
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
                        static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType.SpecFieldType.PercentileFieldType create() {
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
                            static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType create() {
                                return Js.uncheckedCast(JsPropertyMap.of());
                            }

                            @JsProperty
                            String getColumnName();

                            @JsProperty
                            void setColumnName(String columnName);
                        }

                        @JsOverlay
                        static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType.SpecFieldType.SortedFirstFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        JsArray<BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType> getColumnsList();

                        @JsOverlay
                        default void setColumnsList(
                                BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType[] columnsList) {
                            setColumnsList(
                                    Js.<JsArray<BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType>>uncheckedCast(
                                            columnsList));
                        }

                        @JsProperty
                        void setColumnsList(
                                JsArray<BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType> columnsList);
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface TDigestFieldType {
                        @JsOverlay
                        static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType.SpecFieldType.TDigestFieldType create() {
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
                            static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType create() {
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
                        static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType.SpecFieldType.UniqueFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType getNonUniqueSentinel();

                        @JsProperty
                        boolean isIncludeNulls();

                        @JsProperty
                        void setIncludeNulls(boolean includeNulls);

                        @JsProperty
                        void setNonUniqueSentinel(
                                BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType nonUniqueSentinel);
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface WeightedAvgFieldType {
                        @JsOverlay
                        static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType.SpecFieldType.WeightedAvgFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        String getWeightColumn();

                        @JsProperty
                        void setWeightColumn(String weightColumn);
                    }

                    @JsOverlay
                    static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType.SpecFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    Object getAbsSum();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType.SpecFieldType.ApproximatePercentileFieldType getApproximatePercentile();

                    @JsProperty
                    Object getAvg();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType.SpecFieldType.CountDistinctFieldType getCountDistinct();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType.SpecFieldType.DistinctFieldType getDistinct();

                    @JsProperty
                    Object getFirst();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType.SpecFieldType.FormulaFieldType getFormula();

                    @JsProperty
                    Object getFreeze();

                    @JsProperty
                    Object getGroup();

                    @JsProperty
                    Object getLast();

                    @JsProperty
                    Object getMax();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType.SpecFieldType.MedianFieldType getMedian();

                    @JsProperty
                    Object getMin();

                    @JsProperty
                    Object getPb_var();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType.SpecFieldType.PercentileFieldType getPercentile();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType.SpecFieldType.SortedFirstFieldType getSortedFirst();

                    @JsProperty
                    Object getSortedLast();

                    @JsProperty
                    Object getStd();

                    @JsProperty
                    Object getSum();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType.SpecFieldType.TDigestFieldType getTDigest();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType.SpecFieldType.UniqueFieldType getUnique();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType.SpecFieldType.WeightedAvgFieldType getWeightedAvg();

                    @JsProperty
                    Object getWeightedSum();

                    @JsProperty
                    void setAbsSum(Object absSum);

                    @JsProperty
                    void setApproximatePercentile(
                            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType.SpecFieldType.ApproximatePercentileFieldType approximatePercentile);

                    @JsProperty
                    void setAvg(Object avg);

                    @JsProperty
                    void setCountDistinct(
                            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType.SpecFieldType.CountDistinctFieldType countDistinct);

                    @JsProperty
                    void setDistinct(
                            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType.SpecFieldType.DistinctFieldType distinct);

                    @JsProperty
                    void setFirst(Object first);

                    @JsProperty
                    void setFormula(
                            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType.SpecFieldType.FormulaFieldType formula);

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
                            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType.SpecFieldType.MedianFieldType median);

                    @JsProperty
                    void setMin(Object min);

                    @JsProperty
                    void setPb_var(Object pb_var);

                    @JsProperty
                    void setPercentile(
                            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType.SpecFieldType.PercentileFieldType percentile);

                    @JsProperty
                    void setSortedFirst(
                            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType.SpecFieldType.SortedFirstFieldType sortedFirst);

                    @JsProperty
                    void setSortedLast(Object sortedLast);

                    @JsProperty
                    void setStd(Object std);

                    @JsProperty
                    void setSum(Object sum);

                    @JsProperty
                    void setTDigest(
                            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType.SpecFieldType.TDigestFieldType tDigest);

                    @JsProperty
                    void setUnique(
                            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType.SpecFieldType.UniqueFieldType unique);

                    @JsProperty
                    void setWeightedAvg(
                            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType.SpecFieldType.WeightedAvgFieldType weightedAvg);

                    @JsProperty
                    void setWeightedSum(Object weightedSum);
                }

                @JsOverlay
                static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<String> getGroupByColumnsList();

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getSourceId();

                @JsProperty
                BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType.SpecFieldType getSpec();

                @JsProperty
                void setGroupByColumnsList(JsArray<String> groupByColumnsList);

                @JsOverlay
                default void setGroupByColumnsList(String[] groupByColumnsList) {
                    setGroupByColumnsList(Js.<JsArray<String>>uncheckedCast(groupByColumnsList));
                }

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setSourceId(Object sourceId);

                @JsProperty
                void setSpec(
                        BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType.SpecFieldType spec);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface AggregateFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface AggregationsListFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface ColumnsFieldType {
                        @JsOverlay
                        static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateFieldType.AggregationsListFieldType.ColumnsFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        JsArray<String> getMatchPairsList();

                        @JsProperty
                        Object getSpec();

                        @JsProperty
                        void setMatchPairsList(JsArray<String> matchPairsList);

                        @JsOverlay
                        default void setMatchPairsList(String[] matchPairsList) {
                            setMatchPairsList(Js.<JsArray<String>>uncheckedCast(matchPairsList));
                        }

                        @JsProperty
                        void setSpec(Object spec);
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface CountFieldType {
                        @JsOverlay
                        static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateFieldType.AggregationsListFieldType.CountFieldType create() {
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
                        static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateFieldType.AggregationsListFieldType.FirstRowKeyFieldType create() {
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
                        static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateFieldType.AggregationsListFieldType.PartitionFieldType create() {
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
                    static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateFieldType.AggregationsListFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateFieldType.AggregationsListFieldType.ColumnsFieldType getColumns();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateFieldType.AggregationsListFieldType.CountFieldType getCount();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateFieldType.AggregationsListFieldType.FirstRowKeyFieldType getFirstRowKey();

                    @JsProperty
                    Object getLastRowKey();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateFieldType.AggregationsListFieldType.PartitionFieldType getPartition();

                    @JsProperty
                    void setColumns(
                            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateFieldType.AggregationsListFieldType.ColumnsFieldType columns);

                    @JsProperty
                    void setCount(
                            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateFieldType.AggregationsListFieldType.CountFieldType count);

                    @JsProperty
                    void setFirstRowKey(
                            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateFieldType.AggregationsListFieldType.FirstRowKeyFieldType firstRowKey);

                    @JsProperty
                    void setLastRowKey(Object lastRowKey);

                    @JsProperty
                    void setPartition(
                            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateFieldType.AggregationsListFieldType.PartitionFieldType partition);
                }

                @JsOverlay
                static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateFieldType.AggregationsListFieldType> getAggregationsList();

                @JsProperty
                JsArray<String> getGroupByColumnsList();

                @JsProperty
                Object getInitialGroupsId();

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getSourceId();

                @JsProperty
                boolean isPreserveEmpty();

                @JsOverlay
                default void setAggregationsList(
                        BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateFieldType.AggregationsListFieldType[] aggregationsList) {
                    setAggregationsList(
                            Js.<JsArray<BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateFieldType.AggregationsListFieldType>>uncheckedCast(
                                    aggregationsList));
                }

                @JsProperty
                void setAggregationsList(
                        JsArray<BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateFieldType.AggregationsListFieldType> aggregationsList);

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
                void setResultId(Object resultId);

                @JsProperty
                void setSourceId(Object sourceId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ApplyPreviewColumnsFieldType {
                @JsOverlay
                static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.ApplyPreviewColumnsFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getSourceId();

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setSourceId(Object sourceId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface AsOfJoinFieldType {
                @JsOverlay
                static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AsOfJoinFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                double getAsOfMatchRule();

                @JsProperty
                JsArray<String> getColumnsToAddList();

                @JsProperty
                JsArray<String> getColumnsToMatchList();

                @JsProperty
                Object getLeftId();

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getRightId();

                @JsProperty
                void setAsOfMatchRule(double asOfMatchRule);

                @JsProperty
                void setColumnsToAddList(JsArray<String> columnsToAddList);

                @JsOverlay
                default void setColumnsToAddList(String[] columnsToAddList) {
                    setColumnsToAddList(Js.<JsArray<String>>uncheckedCast(columnsToAddList));
                }

                @JsProperty
                void setColumnsToMatchList(JsArray<String> columnsToMatchList);

                @JsOverlay
                default void setColumnsToMatchList(String[] columnsToMatchList) {
                    setColumnsToMatchList(Js.<JsArray<String>>uncheckedCast(columnsToMatchList));
                }

                @JsProperty
                void setLeftId(Object leftId);

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setRightId(Object rightId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ComboAggregateFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface AggregatesListFieldType {
                    @JsOverlay
                    static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.ComboAggregateFieldType.AggregatesListFieldType create() {
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

                @JsOverlay
                static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.ComboAggregateFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<BatchTableRequest.ToObjectReturnType0.OpsListFieldType.ComboAggregateFieldType.AggregatesListFieldType> getAggregatesList();

                @JsProperty
                JsArray<String> getGroupByColumnsList();

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getSourceId();

                @JsProperty
                boolean isForceCombo();

                @JsOverlay
                default void setAggregatesList(
                        BatchTableRequest.ToObjectReturnType0.OpsListFieldType.ComboAggregateFieldType.AggregatesListFieldType[] aggregatesList) {
                    setAggregatesList(
                            Js.<JsArray<BatchTableRequest.ToObjectReturnType0.OpsListFieldType.ComboAggregateFieldType.AggregatesListFieldType>>uncheckedCast(
                                    aggregatesList));
                }

                @JsProperty
                void setAggregatesList(
                        JsArray<BatchTableRequest.ToObjectReturnType0.OpsListFieldType.ComboAggregateFieldType.AggregatesListFieldType> aggregatesList);

                @JsProperty
                void setForceCombo(boolean forceCombo);

                @JsProperty
                void setGroupByColumnsList(JsArray<String> groupByColumnsList);

                @JsOverlay
                default void setGroupByColumnsList(String[] groupByColumnsList) {
                    setGroupByColumnsList(Js.<JsArray<String>>uncheckedCast(groupByColumnsList));
                }

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setSourceId(Object sourceId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface CreateInputTableFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface GetSchemaUnionType {
                    @JsOverlay
                    static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.CreateInputTableFieldType.GetSchemaUnionType of(
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

                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface KindFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface InMemoryKeyBackedFieldType {
                        @JsOverlay
                        static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.CreateInputTableFieldType.KindFieldType.InMemoryKeyBackedFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        JsArray<String> getKeyColumnsList();

                        @JsProperty
                        void setKeyColumnsList(JsArray<String> keyColumnsList);

                        @JsOverlay
                        default void setKeyColumnsList(String[] keyColumnsList) {
                            setKeyColumnsList(Js.<JsArray<String>>uncheckedCast(keyColumnsList));
                        }
                    }

                    @JsOverlay
                    static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.CreateInputTableFieldType.KindFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    Object getInMemoryAppendOnly();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.CreateInputTableFieldType.KindFieldType.InMemoryKeyBackedFieldType getInMemoryKeyBacked();

                    @JsProperty
                    void setInMemoryAppendOnly(Object inMemoryAppendOnly);

                    @JsProperty
                    void setInMemoryKeyBacked(
                            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.CreateInputTableFieldType.KindFieldType.InMemoryKeyBackedFieldType inMemoryKeyBacked);
                }

                @JsOverlay
                static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.CreateInputTableFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                BatchTableRequest.ToObjectReturnType0.OpsListFieldType.CreateInputTableFieldType.KindFieldType getKind();

                @JsProperty
                Object getResultId();

                @JsProperty
                BatchTableRequest.ToObjectReturnType0.OpsListFieldType.CreateInputTableFieldType.GetSchemaUnionType getSchema();

                @JsProperty
                Object getSourceTableId();

                @JsProperty
                void setKind(
                        BatchTableRequest.ToObjectReturnType0.OpsListFieldType.CreateInputTableFieldType.KindFieldType kind);

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setSchema(
                        BatchTableRequest.ToObjectReturnType0.OpsListFieldType.CreateInputTableFieldType.GetSchemaUnionType schema);

                @JsOverlay
                default void setSchema(String schema) {
                    setSchema(
                            Js.<BatchTableRequest.ToObjectReturnType0.OpsListFieldType.CreateInputTableFieldType.GetSchemaUnionType>uncheckedCast(
                                    schema));
                }

                @JsOverlay
                default void setSchema(Uint8Array schema) {
                    setSchema(
                            Js.<BatchTableRequest.ToObjectReturnType0.OpsListFieldType.CreateInputTableFieldType.GetSchemaUnionType>uncheckedCast(
                                    schema));
                }

                @JsProperty
                void setSourceTableId(Object sourceTableId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface CrossJoinFieldType {
                @JsOverlay
                static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.CrossJoinFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<String> getColumnsToAddList();

                @JsProperty
                JsArray<String> getColumnsToMatchList();

                @JsProperty
                Object getLeftId();

                @JsProperty
                double getReserveBits();

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getRightId();

                @JsProperty
                void setColumnsToAddList(JsArray<String> columnsToAddList);

                @JsOverlay
                default void setColumnsToAddList(String[] columnsToAddList) {
                    setColumnsToAddList(Js.<JsArray<String>>uncheckedCast(columnsToAddList));
                }

                @JsProperty
                void setColumnsToMatchList(JsArray<String> columnsToMatchList);

                @JsOverlay
                default void setColumnsToMatchList(String[] columnsToMatchList) {
                    setColumnsToMatchList(Js.<JsArray<String>>uncheckedCast(columnsToMatchList));
                }

                @JsProperty
                void setLeftId(Object leftId);

                @JsProperty
                void setReserveBits(double reserveBits);

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setRightId(Object rightId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface DropColumnsFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface SourceIdFieldType {
                    @JsOverlay
                    static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.DropColumnsFieldType.SourceIdFieldType create() {
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
                static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.DropColumnsFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<String> getColumnNamesList();

                @JsProperty
                Object getResultId();

                @JsProperty
                BatchTableRequest.ToObjectReturnType0.OpsListFieldType.DropColumnsFieldType.SourceIdFieldType getSourceId();

                @JsProperty
                void setColumnNamesList(JsArray<String> columnNamesList);

                @JsOverlay
                default void setColumnNamesList(String[] columnNamesList) {
                    setColumnNamesList(Js.<JsArray<String>>uncheckedCast(columnNamesList));
                }

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setSourceId(
                        BatchTableRequest.ToObjectReturnType0.OpsListFieldType.DropColumnsFieldType.SourceIdFieldType sourceId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface EmptyTableFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface ResultIdFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface GetTicketUnionType {
                        @JsOverlay
                        static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.EmptyTableFieldType.ResultIdFieldType.GetTicketUnionType of(
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
                    static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.EmptyTableFieldType.ResultIdFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.EmptyTableFieldType.ResultIdFieldType.GetTicketUnionType getTicket();

                    @JsProperty
                    void setTicket(
                            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.EmptyTableFieldType.ResultIdFieldType.GetTicketUnionType ticket);

                    @JsOverlay
                    default void setTicket(String ticket) {
                        setTicket(
                                Js.<BatchTableRequest.ToObjectReturnType0.OpsListFieldType.EmptyTableFieldType.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                        ticket));
                    }

                    @JsOverlay
                    default void setTicket(Uint8Array ticket) {
                        setTicket(
                                Js.<BatchTableRequest.ToObjectReturnType0.OpsListFieldType.EmptyTableFieldType.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                        ticket));
                    }
                }

                @JsOverlay
                static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.EmptyTableFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                BatchTableRequest.ToObjectReturnType0.OpsListFieldType.EmptyTableFieldType.ResultIdFieldType getResultId();

                @JsProperty
                String getSize();

                @JsProperty
                void setResultId(
                        BatchTableRequest.ToObjectReturnType0.OpsListFieldType.EmptyTableFieldType.ResultIdFieldType resultId);

                @JsProperty
                void setSize(String size);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ExactJoinFieldType {
                @JsOverlay
                static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.ExactJoinFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<String> getColumnsToAddList();

                @JsProperty
                JsArray<String> getColumnsToMatchList();

                @JsProperty
                Object getLeftId();

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getRightId();

                @JsProperty
                void setColumnsToAddList(JsArray<String> columnsToAddList);

                @JsOverlay
                default void setColumnsToAddList(String[] columnsToAddList) {
                    setColumnsToAddList(Js.<JsArray<String>>uncheckedCast(columnsToAddList));
                }

                @JsProperty
                void setColumnsToMatchList(JsArray<String> columnsToMatchList);

                @JsOverlay
                default void setColumnsToMatchList(String[] columnsToMatchList) {
                    setColumnsToMatchList(Js.<JsArray<String>>uncheckedCast(columnsToMatchList));
                }

                @JsProperty
                void setLeftId(Object leftId);

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setRightId(Object rightId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface FetchTableFieldType {
                @JsOverlay
                static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FetchTableFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getSourceId();

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setSourceId(Object sourceId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface FilterFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface FiltersListFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface AndFieldType {
                        @JsOverlay
                        static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType.AndFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        JsArray<Object> getFiltersList();

                        @JsProperty
                        void setFiltersList(JsArray<Object> filtersList);

                        @JsOverlay
                        default void setFiltersList(Object[] filtersList) {
                            setFiltersList(Js.<JsArray<Object>>uncheckedCast(filtersList));
                        }
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface CompareFieldType {
                        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                        public interface LhsFieldType {
                            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                            public interface LiteralFieldType {
                                @JsOverlay
                                static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType.CompareFieldType.LhsFieldType.LiteralFieldType create() {
                                    return Js.uncheckedCast(JsPropertyMap.of());
                                }

                                @JsProperty
                                double getDoubleValue();

                                @JsProperty
                                String getLongValue();

                                @JsProperty
                                String getNanoTimeValue();

                                @JsProperty
                                String getStringValue();

                                @JsProperty
                                boolean isBoolValue();

                                @JsProperty
                                void setBoolValue(boolean boolValue);

                                @JsProperty
                                void setDoubleValue(double doubleValue);

                                @JsProperty
                                void setLongValue(String longValue);

                                @JsProperty
                                void setNanoTimeValue(String nanoTimeValue);

                                @JsProperty
                                void setStringValue(String stringValue);
                            }

                            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                            public interface ReferenceFieldType {
                                @JsOverlay
                                static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType.CompareFieldType.LhsFieldType.ReferenceFieldType create() {
                                    return Js.uncheckedCast(JsPropertyMap.of());
                                }

                                @JsProperty
                                String getColumnName();

                                @JsProperty
                                void setColumnName(String columnName);
                            }

                            @JsOverlay
                            static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType.CompareFieldType.LhsFieldType create() {
                                return Js.uncheckedCast(JsPropertyMap.of());
                            }

                            @JsProperty
                            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType.CompareFieldType.LhsFieldType.LiteralFieldType getLiteral();

                            @JsProperty
                            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType.CompareFieldType.LhsFieldType.ReferenceFieldType getReference();

                            @JsProperty
                            void setLiteral(
                                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType.CompareFieldType.LhsFieldType.LiteralFieldType literal);

                            @JsProperty
                            void setReference(
                                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType.CompareFieldType.LhsFieldType.ReferenceFieldType reference);
                        }

                        @JsOverlay
                        static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType.CompareFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        double getCaseSensitivity();

                        @JsProperty
                        BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType.CompareFieldType.LhsFieldType getLhs();

                        @JsProperty
                        double getOperation();

                        @JsProperty
                        Object getRhs();

                        @JsProperty
                        void setCaseSensitivity(double caseSensitivity);

                        @JsProperty
                        void setLhs(
                                BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType.CompareFieldType.LhsFieldType lhs);

                        @JsProperty
                        void setOperation(double operation);

                        @JsProperty
                        void setRhs(Object rhs);
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface ContainsFieldType {
                        @JsOverlay
                        static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType.ContainsFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        double getCaseSensitivity();

                        @JsProperty
                        double getMatchType();

                        @JsProperty
                        Object getReference();

                        @JsProperty
                        String getSearchString();

                        @JsProperty
                        void setCaseSensitivity(double caseSensitivity);

                        @JsProperty
                        void setMatchType(double matchType);

                        @JsProperty
                        void setReference(Object reference);

                        @JsProperty
                        void setSearchString(String searchString);
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface InvokeFieldType {
                        @JsOverlay
                        static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType.InvokeFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        JsArray<Object> getArgumentsList();

                        @JsProperty
                        String getMethod();

                        @JsProperty
                        Object getTarget();

                        @JsProperty
                        void setArgumentsList(JsArray<Object> argumentsList);

                        @JsOverlay
                        default void setArgumentsList(Object[] argumentsList) {
                            setArgumentsList(Js.<JsArray<Object>>uncheckedCast(argumentsList));
                        }

                        @JsProperty
                        void setMethod(String method);

                        @JsProperty
                        void setTarget(Object target);
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface IsNullFieldType {
                        @JsOverlay
                        static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType.IsNullFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        Object getReference();

                        @JsProperty
                        void setReference(Object reference);
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface MatchesFieldType {
                        @JsOverlay
                        static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType.MatchesFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        double getCaseSensitivity();

                        @JsProperty
                        double getMatchType();

                        @JsProperty
                        Object getReference();

                        @JsProperty
                        String getRegex();

                        @JsProperty
                        void setCaseSensitivity(double caseSensitivity);

                        @JsProperty
                        void setMatchType(double matchType);

                        @JsProperty
                        void setReference(Object reference);

                        @JsProperty
                        void setRegex(String regex);
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface NotFieldType {
                        @JsOverlay
                        static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType.NotFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        Object getFilter();

                        @JsProperty
                        void setFilter(Object filter);
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface OrFieldType {
                        @JsOverlay
                        static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType.OrFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        JsArray<Object> getFiltersList();

                        @JsProperty
                        void setFiltersList(JsArray<Object> filtersList);

                        @JsOverlay
                        default void setFiltersList(Object[] filtersList) {
                            setFiltersList(Js.<JsArray<Object>>uncheckedCast(filtersList));
                        }
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface Pb_inFieldType {
                        @JsOverlay
                        static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType.Pb_inFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        JsArray<Object> getCandidatesList();

                        @JsProperty
                        double getCaseSensitivity();

                        @JsProperty
                        double getMatchType();

                        @JsProperty
                        Object getTarget();

                        @JsProperty
                        void setCandidatesList(JsArray<Object> candidatesList);

                        @JsOverlay
                        default void setCandidatesList(Object[] candidatesList) {
                            setCandidatesList(Js.<JsArray<Object>>uncheckedCast(candidatesList));
                        }

                        @JsProperty
                        void setCaseSensitivity(double caseSensitivity);

                        @JsProperty
                        void setMatchType(double matchType);

                        @JsProperty
                        void setTarget(Object target);
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface SearchFieldType {
                        @JsOverlay
                        static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType.SearchFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        JsArray<Object> getOptionalReferencesList();

                        @JsProperty
                        String getSearchString();

                        @JsProperty
                        void setOptionalReferencesList(JsArray<Object> optionalReferencesList);

                        @JsOverlay
                        default void setOptionalReferencesList(Object[] optionalReferencesList) {
                            setOptionalReferencesList(Js.<JsArray<Object>>uncheckedCast(optionalReferencesList));
                        }

                        @JsProperty
                        void setSearchString(String searchString);
                    }

                    @JsOverlay
                    static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType.AndFieldType getAnd();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType.CompareFieldType getCompare();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType.ContainsFieldType getContains();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType.InvokeFieldType getInvoke();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType.IsNullFieldType getIsNull();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType.MatchesFieldType getMatches();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType.NotFieldType getNot();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType.OrFieldType getOr();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType.Pb_inFieldType getPb_in();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType.SearchFieldType getSearch();

                    @JsProperty
                    void setAnd(
                            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType.AndFieldType and);

                    @JsProperty
                    void setCompare(
                            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType.CompareFieldType compare);

                    @JsProperty
                    void setContains(
                            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType.ContainsFieldType contains);

                    @JsProperty
                    void setInvoke(
                            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType.InvokeFieldType invoke);

                    @JsProperty
                    void setIsNull(
                            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType.IsNullFieldType isNull);

                    @JsProperty
                    void setMatches(
                            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType.MatchesFieldType matches);

                    @JsProperty
                    void setNot(
                            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType.NotFieldType not);

                    @JsProperty
                    void setOr(
                            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType.OrFieldType or);

                    @JsProperty
                    void setPb_in(
                            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType.Pb_inFieldType pb_in);

                    @JsProperty
                    void setSearch(
                            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType.SearchFieldType search);
                }

                @JsOverlay
                static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType> getFiltersList();

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getSourceId();

                @JsOverlay
                default void setFiltersList(
                        BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType[] filtersList) {
                    setFiltersList(
                            Js.<JsArray<BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType>>uncheckedCast(
                                    filtersList));
                }

                @JsProperty
                void setFiltersList(
                        JsArray<BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType.FiltersListFieldType> filtersList);

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setSourceId(Object sourceId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface FlattenFieldType {
                @JsOverlay
                static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FlattenFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getSourceId();

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setSourceId(Object sourceId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface HeadByFieldType {
                @JsOverlay
                static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.HeadByFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<String> getGroupByColumnSpecsList();

                @JsProperty
                String getNumRows();

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getSourceId();

                @JsProperty
                void setGroupByColumnSpecsList(JsArray<String> groupByColumnSpecsList);

                @JsOverlay
                default void setGroupByColumnSpecsList(String[] groupByColumnSpecsList) {
                    setGroupByColumnSpecsList(Js.<JsArray<String>>uncheckedCast(groupByColumnSpecsList));
                }

                @JsProperty
                void setNumRows(String numRows);

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setSourceId(Object sourceId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface HeadFieldType {
                @JsOverlay
                static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.HeadFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getNumRows();

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getSourceId();

                @JsProperty
                void setNumRows(String numRows);

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setSourceId(Object sourceId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface LeftJoinFieldType {
                @JsOverlay
                static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.LeftJoinFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<String> getColumnsToAddList();

                @JsProperty
                JsArray<String> getColumnsToMatchList();

                @JsProperty
                Object getLeftId();

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getRightId();

                @JsProperty
                void setColumnsToAddList(JsArray<String> columnsToAddList);

                @JsOverlay
                default void setColumnsToAddList(String[] columnsToAddList) {
                    setColumnsToAddList(Js.<JsArray<String>>uncheckedCast(columnsToAddList));
                }

                @JsProperty
                void setColumnsToMatchList(JsArray<String> columnsToMatchList);

                @JsOverlay
                default void setColumnsToMatchList(String[] columnsToMatchList) {
                    setColumnsToMatchList(Js.<JsArray<String>>uncheckedCast(columnsToMatchList));
                }

                @JsProperty
                void setLeftId(Object leftId);

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setRightId(Object rightId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface MergeFieldType {
                @JsOverlay
                static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.MergeFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getKeyColumn();

                @JsProperty
                Object getResultId();

                @JsProperty
                JsArray<Object> getSourceIdsList();

                @JsProperty
                void setKeyColumn(String keyColumn);

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setSourceIdsList(JsArray<Object> sourceIdsList);

                @JsOverlay
                default void setSourceIdsList(Object[] sourceIdsList) {
                    setSourceIdsList(Js.<JsArray<Object>>uncheckedCast(sourceIdsList));
                }
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface NaturalJoinFieldType {
                @JsOverlay
                static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.NaturalJoinFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<String> getColumnsToAddList();

                @JsProperty
                JsArray<String> getColumnsToMatchList();

                @JsProperty
                Object getLeftId();

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getRightId();

                @JsProperty
                void setColumnsToAddList(JsArray<String> columnsToAddList);

                @JsOverlay
                default void setColumnsToAddList(String[] columnsToAddList) {
                    setColumnsToAddList(Js.<JsArray<String>>uncheckedCast(columnsToAddList));
                }

                @JsProperty
                void setColumnsToMatchList(JsArray<String> columnsToMatchList);

                @JsOverlay
                default void setColumnsToMatchList(String[] columnsToMatchList) {
                    setColumnsToMatchList(Js.<JsArray<String>>uncheckedCast(columnsToMatchList));
                }

                @JsProperty
                void setLeftId(Object leftId);

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setRightId(Object rightId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface RunChartDownsampleFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface ZoomRangeFieldType {
                    @JsOverlay
                    static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.RunChartDownsampleFieldType.ZoomRangeFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    String getMaxDateNanos();

                    @JsProperty
                    String getMinDateNanos();

                    @JsProperty
                    void setMaxDateNanos(String maxDateNanos);

                    @JsProperty
                    void setMinDateNanos(String minDateNanos);
                }

                @JsOverlay
                static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.RunChartDownsampleFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                double getPixelCount();

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getSourceId();

                @JsProperty
                String getXColumnName();

                @JsProperty
                JsArray<String> getYColumnNamesList();

                @JsProperty
                BatchTableRequest.ToObjectReturnType0.OpsListFieldType.RunChartDownsampleFieldType.ZoomRangeFieldType getZoomRange();

                @JsProperty
                void setPixelCount(double pixelCount);

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setSourceId(Object sourceId);

                @JsProperty
                void setXColumnName(String xColumnName);

                @JsProperty
                void setYColumnNamesList(JsArray<String> yColumnNamesList);

                @JsOverlay
                default void setYColumnNamesList(String[] yColumnNamesList) {
                    setYColumnNamesList(Js.<JsArray<String>>uncheckedCast(yColumnNamesList));
                }

                @JsProperty
                void setZoomRange(
                        BatchTableRequest.ToObjectReturnType0.OpsListFieldType.RunChartDownsampleFieldType.ZoomRangeFieldType zoomRange);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface SelectDistinctFieldType {
                @JsOverlay
                static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.SelectDistinctFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<String> getColumnNamesList();

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getSourceId();

                @JsProperty
                void setColumnNamesList(JsArray<String> columnNamesList);

                @JsOverlay
                default void setColumnNamesList(String[] columnNamesList) {
                    setColumnNamesList(Js.<JsArray<String>>uncheckedCast(columnNamesList));
                }

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setSourceId(Object sourceId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface SnapshotFieldType {
                @JsOverlay
                static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.SnapshotFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getSourceId();

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setSourceId(Object sourceId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface SnapshotWhenFieldType {
                @JsOverlay
                static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.SnapshotWhenFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getBaseId();

                @JsProperty
                Object getResultId();

                @JsProperty
                JsArray<String> getStampColumnsList();

                @JsProperty
                Object getTriggerId();

                @JsProperty
                boolean isHistory();

                @JsProperty
                boolean isIncremental();

                @JsProperty
                boolean isInitial();

                @JsProperty
                void setBaseId(Object baseId);

                @JsProperty
                void setHistory(boolean history);

                @JsProperty
                void setIncremental(boolean incremental);

                @JsProperty
                void setInitial(boolean initial);

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setStampColumnsList(JsArray<String> stampColumnsList);

                @JsOverlay
                default void setStampColumnsList(String[] stampColumnsList) {
                    setStampColumnsList(Js.<JsArray<String>>uncheckedCast(stampColumnsList));
                }

                @JsProperty
                void setTriggerId(Object triggerId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface SortFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface SortsListFieldType {
                    @JsOverlay
                    static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.SortFieldType.SortsListFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    String getColumnName();

                    @JsProperty
                    double getDirection();

                    @JsProperty
                    boolean isIsAbsolute();

                    @JsProperty
                    void setColumnName(String columnName);

                    @JsProperty
                    void setDirection(double direction);

                    @JsProperty
                    void setIsAbsolute(boolean isAbsolute);
                }

                @JsOverlay
                static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.SortFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getResultId();

                @JsProperty
                JsArray<BatchTableRequest.ToObjectReturnType0.OpsListFieldType.SortFieldType.SortsListFieldType> getSortsList();

                @JsProperty
                Object getSourceId();

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setSortsList(
                        JsArray<BatchTableRequest.ToObjectReturnType0.OpsListFieldType.SortFieldType.SortsListFieldType> sortsList);

                @JsOverlay
                default void setSortsList(
                        BatchTableRequest.ToObjectReturnType0.OpsListFieldType.SortFieldType.SortsListFieldType[] sortsList) {
                    setSortsList(
                            Js.<JsArray<BatchTableRequest.ToObjectReturnType0.OpsListFieldType.SortFieldType.SortsListFieldType>>uncheckedCast(
                                    sortsList));
                }

                @JsProperty
                void setSourceId(Object sourceId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TimeTableFieldType {
                @JsOverlay
                static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.TimeTableFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getPeriodNanos();

                @JsProperty
                Object getResultId();

                @JsProperty
                String getStartTimeNanos();

                @JsProperty
                void setPeriodNanos(String periodNanos);

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setStartTimeNanos(String startTimeNanos);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface UngroupFieldType {
                @JsOverlay
                static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UngroupFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<String> getColumnsToUngroupList();

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getSourceId();

                @JsProperty
                boolean isNullFill();

                @JsProperty
                void setColumnsToUngroupList(JsArray<String> columnsToUngroupList);

                @JsOverlay
                default void setColumnsToUngroupList(String[] columnsToUngroupList) {
                    setColumnsToUngroupList(Js.<JsArray<String>>uncheckedCast(columnsToUngroupList));
                }

                @JsProperty
                void setNullFill(boolean nullFill);

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setSourceId(Object sourceId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface UnstructuredFilterFieldType {
                @JsOverlay
                static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UnstructuredFilterFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<String> getFiltersList();

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getSourceId();

                @JsProperty
                void setFiltersList(JsArray<String> filtersList);

                @JsOverlay
                default void setFiltersList(String[] filtersList) {
                    setFiltersList(Js.<JsArray<String>>uncheckedCast(filtersList));
                }

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setSourceId(Object sourceId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface UpdateByFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface OperationsListFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface ColumnFieldType {
                        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                        public interface SpecFieldType {
                            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                            public interface EmaFieldType {
                                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                                public interface OptionsFieldType {
                                    @JsOverlay
                                    static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.OptionsFieldType create() {
                                        return Js.uncheckedCast(JsPropertyMap.of());
                                    }

                                    @JsProperty
                                    Object getBigValueContext();

                                    @JsProperty
                                    double getOnNanValue();

                                    @JsProperty
                                    double getOnNegativeDeltaTime();

                                    @JsProperty
                                    double getOnNullTime();

                                    @JsProperty
                                    double getOnNullValue();

                                    @JsProperty
                                    double getOnZeroDeltaTime();

                                    @JsProperty
                                    void setBigValueContext(Object bigValueContext);

                                    @JsProperty
                                    void setOnNanValue(double onNanValue);

                                    @JsProperty
                                    void setOnNegativeDeltaTime(double onNegativeDeltaTime);

                                    @JsProperty
                                    void setOnNullTime(double onNullTime);

                                    @JsProperty
                                    void setOnNullValue(double onNullValue);

                                    @JsProperty
                                    void setOnZeroDeltaTime(double onZeroDeltaTime);
                                }

                                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                                public interface TimescaleFieldType {
                                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                                    public interface TicksFieldType {
                                        @JsOverlay
                                        static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TicksFieldType create() {
                                            return Js.uncheckedCast(JsPropertyMap.of());
                                        }

                                        @JsProperty
                                        String getTicks();

                                        @JsProperty
                                        void setTicks(String ticks);
                                    }

                                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                                    public interface TimeFieldType {
                                        @JsOverlay
                                        static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TimeFieldType create() {
                                            return Js.uncheckedCast(JsPropertyMap.of());
                                        }

                                        @JsProperty
                                        String getColumn();

                                        @JsProperty
                                        String getPeriodNanos();

                                        @JsProperty
                                        void setColumn(String column);

                                        @JsProperty
                                        void setPeriodNanos(String periodNanos);
                                    }

                                    @JsOverlay
                                    static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType create() {
                                        return Js.uncheckedCast(JsPropertyMap.of());
                                    }

                                    @JsProperty
                                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TicksFieldType getTicks();

                                    @JsProperty
                                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TimeFieldType getTime();

                                    @JsProperty
                                    void setTicks(
                                            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TicksFieldType ticks);

                                    @JsProperty
                                    void setTime(
                                            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TimeFieldType time);
                                }

                                @JsOverlay
                                static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType create() {
                                    return Js.uncheckedCast(JsPropertyMap.of());
                                }

                                @JsProperty
                                BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.OptionsFieldType getOptions();

                                @JsProperty
                                BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType getTimescale();

                                @JsProperty
                                void setOptions(
                                        BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.OptionsFieldType options);

                                @JsProperty
                                void setTimescale(
                                        BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType timescale);
                            }

                            @JsOverlay
                            static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType create() {
                                return Js.uncheckedCast(JsPropertyMap.of());
                            }

                            @JsProperty
                            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType getEma();

                            @JsProperty
                            Object getFill();

                            @JsProperty
                            Object getMax();

                            @JsProperty
                            Object getMin();

                            @JsProperty
                            Object getProduct();

                            @JsProperty
                            Object getSum();

                            @JsProperty
                            void setEma(
                                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType ema);

                            @JsProperty
                            void setFill(Object fill);

                            @JsProperty
                            void setMax(Object max);

                            @JsProperty
                            void setMin(Object min);

                            @JsProperty
                            void setProduct(Object product);

                            @JsProperty
                            void setSum(Object sum);
                        }

                        @JsOverlay
                        static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        JsArray<String> getMatchPairsList();

                        @JsProperty
                        BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType getSpec();

                        @JsProperty
                        void setMatchPairsList(JsArray<String> matchPairsList);

                        @JsOverlay
                        default void setMatchPairsList(String[] matchPairsList) {
                            setMatchPairsList(Js.<JsArray<String>>uncheckedCast(matchPairsList));
                        }

                        @JsProperty
                        void setSpec(
                                BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType spec);
                    }

                    @JsOverlay
                    static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UpdateByFieldType.OperationsListFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType getColumn();

                    @JsProperty
                    void setColumn(
                            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType column);
                }

                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface OptionsFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface MathContextFieldType {
                        @JsOverlay
                        static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UpdateByFieldType.OptionsFieldType.MathContextFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        double getPrecision();

                        @JsProperty
                        double getRoundingMode();

                        @JsProperty
                        void setPrecision(double precision);

                        @JsProperty
                        void setRoundingMode(double roundingMode);
                    }

                    @JsOverlay
                    static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UpdateByFieldType.OptionsFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    double getChunkCapacity();

                    @JsProperty
                    double getInitialHashTableSize();

                    @JsProperty
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UpdateByFieldType.OptionsFieldType.MathContextFieldType getMathContext();

                    @JsProperty
                    double getMaxStaticSparseMemoryOverhead();

                    @JsProperty
                    double getMaximumLoadFactor();

                    @JsProperty
                    double getTargetLoadFactor();

                    @JsProperty
                    boolean isUseRedirection();

                    @JsProperty
                    void setChunkCapacity(double chunkCapacity);

                    @JsProperty
                    void setInitialHashTableSize(double initialHashTableSize);

                    @JsProperty
                    void setMathContext(
                            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UpdateByFieldType.OptionsFieldType.MathContextFieldType mathContext);

                    @JsProperty
                    void setMaxStaticSparseMemoryOverhead(double maxStaticSparseMemoryOverhead);

                    @JsProperty
                    void setMaximumLoadFactor(double maximumLoadFactor);

                    @JsProperty
                    void setTargetLoadFactor(double targetLoadFactor);

                    @JsProperty
                    void setUseRedirection(boolean useRedirection);
                }

                @JsOverlay
                static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UpdateByFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<String> getGroupByColumnsList();

                @JsProperty
                JsArray<BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UpdateByFieldType.OperationsListFieldType> getOperationsList();

                @JsProperty
                BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UpdateByFieldType.OptionsFieldType getOptions();

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getSourceId();

                @JsProperty
                void setGroupByColumnsList(JsArray<String> groupByColumnsList);

                @JsOverlay
                default void setGroupByColumnsList(String[] groupByColumnsList) {
                    setGroupByColumnsList(Js.<JsArray<String>>uncheckedCast(groupByColumnsList));
                }

                @JsProperty
                void setOperationsList(
                        JsArray<BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UpdateByFieldType.OperationsListFieldType> operationsList);

                @JsOverlay
                default void setOperationsList(
                        BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UpdateByFieldType.OperationsListFieldType[] operationsList) {
                    setOperationsList(
                            Js.<JsArray<BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UpdateByFieldType.OperationsListFieldType>>uncheckedCast(
                                    operationsList));
                }

                @JsProperty
                void setOptions(
                        BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UpdateByFieldType.OptionsFieldType options);

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setSourceId(Object sourceId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface UpdateFieldType {
                @JsOverlay
                static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UpdateFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<String> getColumnSpecsList();

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getSourceId();

                @JsProperty
                void setColumnSpecsList(JsArray<String> columnSpecsList);

                @JsOverlay
                default void setColumnSpecsList(String[] columnSpecsList) {
                    setColumnSpecsList(Js.<JsArray<String>>uncheckedCast(columnSpecsList));
                }

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setSourceId(Object sourceId);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface WhereInFieldType {
                @JsOverlay
                static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.WhereInFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<String> getColumnsToMatchList();

                @JsProperty
                Object getLeftId();

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getRightId();

                @JsProperty
                boolean isInverted();

                @JsProperty
                void setColumnsToMatchList(JsArray<String> columnsToMatchList);

                @JsOverlay
                default void setColumnsToMatchList(String[] columnsToMatchList) {
                    setColumnsToMatchList(Js.<JsArray<String>>uncheckedCast(columnsToMatchList));
                }

                @JsProperty
                void setInverted(boolean inverted);

                @JsProperty
                void setLeftId(Object leftId);

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setRightId(Object rightId);
            }

            @JsOverlay
            static BatchTableRequest.ToObjectReturnType0.OpsListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateFieldType getAggregate();

            @JsProperty
            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType getAggregateAll();

            @JsProperty
            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.ApplyPreviewColumnsFieldType getApplyPreviewColumns();

            @JsProperty
            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AsOfJoinFieldType getAsOfJoin();

            @JsProperty
            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.ComboAggregateFieldType getComboAggregate();

            @JsProperty
            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.CreateInputTableFieldType getCreateInputTable();

            @JsProperty
            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.CrossJoinFieldType getCrossJoin();

            @JsProperty
            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.DropColumnsFieldType getDropColumns();

            @JsProperty
            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.EmptyTableFieldType getEmptyTable();

            @JsProperty
            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.ExactJoinFieldType getExactJoin();

            @JsProperty
            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FetchTableFieldType getFetchTable();

            @JsProperty
            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType getFilter();

            @JsProperty
            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FlattenFieldType getFlatten();

            @JsProperty
            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.HeadFieldType getHead();

            @JsProperty
            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.HeadByFieldType getHeadBy();

            @JsProperty
            Object getLazyUpdate();

            @JsProperty
            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.LeftJoinFieldType getLeftJoin();

            @JsProperty
            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.MergeFieldType getMerge();

            @JsProperty
            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.NaturalJoinFieldType getNaturalJoin();

            @JsProperty
            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.RunChartDownsampleFieldType getRunChartDownsample();

            @JsProperty
            Object getSelect();

            @JsProperty
            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.SelectDistinctFieldType getSelectDistinct();

            @JsProperty
            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.SnapshotFieldType getSnapshot();

            @JsProperty
            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.SnapshotWhenFieldType getSnapshotWhen();

            @JsProperty
            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.SortFieldType getSort();

            @JsProperty
            Object getTail();

            @JsProperty
            Object getTailBy();

            @JsProperty
            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.TimeTableFieldType getTimeTable();

            @JsProperty
            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UngroupFieldType getUngroup();

            @JsProperty
            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UnstructuredFilterFieldType getUnstructuredFilter();

            @JsProperty
            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UpdateFieldType getUpdate();

            @JsProperty
            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UpdateByFieldType getUpdateBy();

            @JsProperty
            Object getUpdateView();

            @JsProperty
            Object getView();

            @JsProperty
            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.WhereInFieldType getWhereIn();

            @JsProperty
            void setAggregate(
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateFieldType aggregate);

            @JsProperty
            void setAggregateAll(
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AggregateAllFieldType aggregateAll);

            @JsProperty
            void setApplyPreviewColumns(
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.ApplyPreviewColumnsFieldType applyPreviewColumns);

            @JsProperty
            void setAsOfJoin(
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AsOfJoinFieldType asOfJoin);

            @JsProperty
            void setComboAggregate(
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.ComboAggregateFieldType comboAggregate);

            @JsProperty
            void setCreateInputTable(
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.CreateInputTableFieldType createInputTable);

            @JsProperty
            void setCrossJoin(
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.CrossJoinFieldType crossJoin);

            @JsProperty
            void setDropColumns(
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.DropColumnsFieldType dropColumns);

            @JsProperty
            void setEmptyTable(
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.EmptyTableFieldType emptyTable);

            @JsProperty
            void setExactJoin(
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.ExactJoinFieldType exactJoin);

            @JsProperty
            void setFetchTable(
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FetchTableFieldType fetchTable);

            @JsProperty
            void setFilter(BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType filter);

            @JsProperty
            void setFlatten(
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FlattenFieldType flatten);

            @JsProperty
            void setHead(BatchTableRequest.ToObjectReturnType0.OpsListFieldType.HeadFieldType head);

            @JsProperty
            void setHeadBy(BatchTableRequest.ToObjectReturnType0.OpsListFieldType.HeadByFieldType headBy);

            @JsProperty
            void setLazyUpdate(Object lazyUpdate);

            @JsProperty
            void setLeftJoin(
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.LeftJoinFieldType leftJoin);

            @JsProperty
            void setMerge(BatchTableRequest.ToObjectReturnType0.OpsListFieldType.MergeFieldType merge);

            @JsProperty
            void setNaturalJoin(
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.NaturalJoinFieldType naturalJoin);

            @JsProperty
            void setRunChartDownsample(
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.RunChartDownsampleFieldType runChartDownsample);

            @JsProperty
            void setSelect(Object select);

            @JsProperty
            void setSelectDistinct(
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.SelectDistinctFieldType selectDistinct);

            @JsProperty
            void setSnapshot(
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.SnapshotFieldType snapshot);

            @JsProperty
            void setSnapshotWhen(
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.SnapshotWhenFieldType snapshotWhen);

            @JsProperty
            void setSort(BatchTableRequest.ToObjectReturnType0.OpsListFieldType.SortFieldType sort);

            @JsProperty
            void setTail(Object tail);

            @JsProperty
            void setTailBy(Object tailBy);

            @JsProperty
            void setTimeTable(
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.TimeTableFieldType timeTable);

            @JsProperty
            void setUngroup(
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UngroupFieldType ungroup);

            @JsProperty
            void setUnstructuredFilter(
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UnstructuredFilterFieldType unstructuredFilter);

            @JsProperty
            void setUpdate(BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UpdateFieldType update);

            @JsProperty
            void setUpdateBy(
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UpdateByFieldType updateBy);

            @JsProperty
            void setUpdateView(Object updateView);

            @JsProperty
            void setView(Object view);

            @JsProperty
            void setWhereIn(
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.WhereInFieldType whereIn);
        }

        @JsOverlay
        static BatchTableRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<BatchTableRequest.ToObjectReturnType0.OpsListFieldType> getOpsList();

        @JsProperty
        void setOpsList(JsArray<BatchTableRequest.ToObjectReturnType0.OpsListFieldType> opsList);

        @JsOverlay
        default void setOpsList(BatchTableRequest.ToObjectReturnType0.OpsListFieldType[] opsList) {
            setOpsList(
                    Js.<JsArray<BatchTableRequest.ToObjectReturnType0.OpsListFieldType>>uncheckedCast(
                            opsList));
        }
    }

    public static native BatchTableRequest deserializeBinary(Uint8Array bytes);

    public static native BatchTableRequest deserializeBinaryFromReader(
            BatchTableRequest message, Object reader);

    public static native void serializeBinaryToWriter(BatchTableRequest message, Object writer);

    public static native BatchTableRequest.ToObjectReturnType toObject(
            boolean includeInstance, BatchTableRequest msg);

    public native Operation addOps();

    public native Operation addOps(Operation value, double index);

    public native Operation addOps(Operation value);

    public native void clearOpsList();

    public native JsArray<Operation> getOpsList();

    public native Uint8Array serializeBinary();

    public native void setOpsList(JsArray<Operation> value);

    @JsOverlay
    public final void setOpsList(Operation[] value) {
        setOpsList(Js.<JsArray<Operation>>uncheckedCast(value));
    }

    public native BatchTableRequest.ToObjectReturnType0 toObject();

    public native BatchTableRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
