//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.batchtablerequest;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.AggregateAllRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.AggregateRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.ApplyPreviewColumnsRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.AsOfJoinTablesRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.ComboAggregateRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.CreateInputTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.CrossJoinTablesRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.DropColumnsRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.EmptyTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.ExactJoinTablesRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.FetchTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.FilterTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.FlattenRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.HeadOrTailByRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.HeadOrTailRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.LeftJoinTablesRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.MergeTablesRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.NaturalJoinTablesRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.RunChartDownsampleRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.SelectDistinctRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.SelectOrUpdateRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.SnapshotTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.SnapshotWhenTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.SortTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.TimeTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.UngroupRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.UnstructuredFilterTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.UpdateByRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.WhereInRequest;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.table_pb.BatchTableRequest.Operation",
        namespace = JsPackage.GLOBAL)
public class Operation {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface AggregateAllFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface SpecFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface ApproximatePercentileFieldType {
                    @JsOverlay
                    static Operation.ToObjectReturnType.AggregateAllFieldType.SpecFieldType.ApproximatePercentileFieldType create() {
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
                    static Operation.ToObjectReturnType.AggregateAllFieldType.SpecFieldType.CountDistinctFieldType create() {
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
                    static Operation.ToObjectReturnType.AggregateAllFieldType.SpecFieldType.DistinctFieldType create() {
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
                    static Operation.ToObjectReturnType.AggregateAllFieldType.SpecFieldType.FormulaFieldType create() {
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
                    static Operation.ToObjectReturnType.AggregateAllFieldType.SpecFieldType.MedianFieldType create() {
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
                    static Operation.ToObjectReturnType.AggregateAllFieldType.SpecFieldType.PercentileFieldType create() {
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
                        static Operation.ToObjectReturnType.AggregateAllFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        String getColumnName();

                        @JsProperty
                        void setColumnName(String columnName);
                    }

                    @JsOverlay
                    static Operation.ToObjectReturnType.AggregateAllFieldType.SpecFieldType.SortedFirstFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    JsArray<Operation.ToObjectReturnType.AggregateAllFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType> getColumnsList();

                    @JsOverlay
                    default void setColumnsList(
                            Operation.ToObjectReturnType.AggregateAllFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType[] columnsList) {
                        setColumnsList(
                                Js.<JsArray<Operation.ToObjectReturnType.AggregateAllFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType>>uncheckedCast(
                                        columnsList));
                    }

                    @JsProperty
                    void setColumnsList(
                            JsArray<Operation.ToObjectReturnType.AggregateAllFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType> columnsList);
                }

                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface TDigestFieldType {
                    @JsOverlay
                    static Operation.ToObjectReturnType.AggregateAllFieldType.SpecFieldType.TDigestFieldType create() {
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
                        static Operation.ToObjectReturnType.AggregateAllFieldType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType create() {
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
                    static Operation.ToObjectReturnType.AggregateAllFieldType.SpecFieldType.UniqueFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    Operation.ToObjectReturnType.AggregateAllFieldType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType getNonUniqueSentinel();

                    @JsProperty
                    boolean isIncludeNulls();

                    @JsProperty
                    void setIncludeNulls(boolean includeNulls);

                    @JsProperty
                    void setNonUniqueSentinel(
                            Operation.ToObjectReturnType.AggregateAllFieldType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType nonUniqueSentinel);
                }

                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface WeightedAvgFieldType {
                    @JsOverlay
                    static Operation.ToObjectReturnType.AggregateAllFieldType.SpecFieldType.WeightedAvgFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    String getWeightColumn();

                    @JsProperty
                    void setWeightColumn(String weightColumn);
                }

                @JsOverlay
                static Operation.ToObjectReturnType.AggregateAllFieldType.SpecFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getAbsSum();

                @JsProperty
                Operation.ToObjectReturnType.AggregateAllFieldType.SpecFieldType.ApproximatePercentileFieldType getApproximatePercentile();

                @JsProperty
                Object getAvg();

                @JsProperty
                Operation.ToObjectReturnType.AggregateAllFieldType.SpecFieldType.CountDistinctFieldType getCountDistinct();

                @JsProperty
                Operation.ToObjectReturnType.AggregateAllFieldType.SpecFieldType.DistinctFieldType getDistinct();

                @JsProperty
                Object getFirst();

                @JsProperty
                Operation.ToObjectReturnType.AggregateAllFieldType.SpecFieldType.FormulaFieldType getFormula();

                @JsProperty
                Object getFreeze();

                @JsProperty
                Object getGroup();

                @JsProperty
                Object getLast();

                @JsProperty
                Object getMax();

                @JsProperty
                Operation.ToObjectReturnType.AggregateAllFieldType.SpecFieldType.MedianFieldType getMedian();

                @JsProperty
                Object getMin();

                @JsProperty
                Object getPb_var();

                @JsProperty
                Operation.ToObjectReturnType.AggregateAllFieldType.SpecFieldType.PercentileFieldType getPercentile();

                @JsProperty
                Operation.ToObjectReturnType.AggregateAllFieldType.SpecFieldType.SortedFirstFieldType getSortedFirst();

                @JsProperty
                Object getSortedLast();

                @JsProperty
                Object getStd();

                @JsProperty
                Object getSum();

                @JsProperty
                Operation.ToObjectReturnType.AggregateAllFieldType.SpecFieldType.TDigestFieldType getTDigest();

                @JsProperty
                Operation.ToObjectReturnType.AggregateAllFieldType.SpecFieldType.UniqueFieldType getUnique();

                @JsProperty
                Operation.ToObjectReturnType.AggregateAllFieldType.SpecFieldType.WeightedAvgFieldType getWeightedAvg();

                @JsProperty
                Object getWeightedSum();

                @JsProperty
                void setAbsSum(Object absSum);

                @JsProperty
                void setApproximatePercentile(
                        Operation.ToObjectReturnType.AggregateAllFieldType.SpecFieldType.ApproximatePercentileFieldType approximatePercentile);

                @JsProperty
                void setAvg(Object avg);

                @JsProperty
                void setCountDistinct(
                        Operation.ToObjectReturnType.AggregateAllFieldType.SpecFieldType.CountDistinctFieldType countDistinct);

                @JsProperty
                void setDistinct(
                        Operation.ToObjectReturnType.AggregateAllFieldType.SpecFieldType.DistinctFieldType distinct);

                @JsProperty
                void setFirst(Object first);

                @JsProperty
                void setFormula(
                        Operation.ToObjectReturnType.AggregateAllFieldType.SpecFieldType.FormulaFieldType formula);

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
                        Operation.ToObjectReturnType.AggregateAllFieldType.SpecFieldType.MedianFieldType median);

                @JsProperty
                void setMin(Object min);

                @JsProperty
                void setPb_var(Object pb_var);

                @JsProperty
                void setPercentile(
                        Operation.ToObjectReturnType.AggregateAllFieldType.SpecFieldType.PercentileFieldType percentile);

                @JsProperty
                void setSortedFirst(
                        Operation.ToObjectReturnType.AggregateAllFieldType.SpecFieldType.SortedFirstFieldType sortedFirst);

                @JsProperty
                void setSortedLast(Object sortedLast);

                @JsProperty
                void setStd(Object std);

                @JsProperty
                void setSum(Object sum);

                @JsProperty
                void setTDigest(
                        Operation.ToObjectReturnType.AggregateAllFieldType.SpecFieldType.TDigestFieldType tDigest);

                @JsProperty
                void setUnique(
                        Operation.ToObjectReturnType.AggregateAllFieldType.SpecFieldType.UniqueFieldType unique);

                @JsProperty
                void setWeightedAvg(
                        Operation.ToObjectReturnType.AggregateAllFieldType.SpecFieldType.WeightedAvgFieldType weightedAvg);

                @JsProperty
                void setWeightedSum(Object weightedSum);
            }

            @JsOverlay
            static Operation.ToObjectReturnType.AggregateAllFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<String> getGroupByColumnsList();

            @JsProperty
            Object getResultId();

            @JsProperty
            Object getSourceId();

            @JsProperty
            Operation.ToObjectReturnType.AggregateAllFieldType.SpecFieldType getSpec();

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
            void setSpec(Operation.ToObjectReturnType.AggregateAllFieldType.SpecFieldType spec);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface AggregateFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface AggregationsListFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface ColumnsFieldType {
                    @JsOverlay
                    static Operation.ToObjectReturnType.AggregateFieldType.AggregationsListFieldType.ColumnsFieldType create() {
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
                    static Operation.ToObjectReturnType.AggregateFieldType.AggregationsListFieldType.CountFieldType create() {
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
                    static Operation.ToObjectReturnType.AggregateFieldType.AggregationsListFieldType.FirstRowKeyFieldType create() {
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
                    static Operation.ToObjectReturnType.AggregateFieldType.AggregationsListFieldType.PartitionFieldType create() {
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
                static Operation.ToObjectReturnType.AggregateFieldType.AggregationsListFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Operation.ToObjectReturnType.AggregateFieldType.AggregationsListFieldType.ColumnsFieldType getColumns();

                @JsProperty
                Operation.ToObjectReturnType.AggregateFieldType.AggregationsListFieldType.CountFieldType getCount();

                @JsProperty
                Operation.ToObjectReturnType.AggregateFieldType.AggregationsListFieldType.FirstRowKeyFieldType getFirstRowKey();

                @JsProperty
                Object getLastRowKey();

                @JsProperty
                Operation.ToObjectReturnType.AggregateFieldType.AggregationsListFieldType.PartitionFieldType getPartition();

                @JsProperty
                void setColumns(
                        Operation.ToObjectReturnType.AggregateFieldType.AggregationsListFieldType.ColumnsFieldType columns);

                @JsProperty
                void setCount(
                        Operation.ToObjectReturnType.AggregateFieldType.AggregationsListFieldType.CountFieldType count);

                @JsProperty
                void setFirstRowKey(
                        Operation.ToObjectReturnType.AggregateFieldType.AggregationsListFieldType.FirstRowKeyFieldType firstRowKey);

                @JsProperty
                void setLastRowKey(Object lastRowKey);

                @JsProperty
                void setPartition(
                        Operation.ToObjectReturnType.AggregateFieldType.AggregationsListFieldType.PartitionFieldType partition);
            }

            @JsOverlay
            static Operation.ToObjectReturnType.AggregateFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<Operation.ToObjectReturnType.AggregateFieldType.AggregationsListFieldType> getAggregationsList();

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
                    Operation.ToObjectReturnType.AggregateFieldType.AggregationsListFieldType[] aggregationsList) {
                setAggregationsList(
                        Js.<JsArray<Operation.ToObjectReturnType.AggregateFieldType.AggregationsListFieldType>>uncheckedCast(
                                aggregationsList));
            }

            @JsProperty
            void setAggregationsList(
                    JsArray<Operation.ToObjectReturnType.AggregateFieldType.AggregationsListFieldType> aggregationsList);

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
            static Operation.ToObjectReturnType.ApplyPreviewColumnsFieldType create() {
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
            static Operation.ToObjectReturnType.AsOfJoinFieldType create() {
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
                static Operation.ToObjectReturnType.ComboAggregateFieldType.AggregatesListFieldType create() {
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
            static Operation.ToObjectReturnType.ComboAggregateFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<Operation.ToObjectReturnType.ComboAggregateFieldType.AggregatesListFieldType> getAggregatesList();

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
                    Operation.ToObjectReturnType.ComboAggregateFieldType.AggregatesListFieldType[] aggregatesList) {
                setAggregatesList(
                        Js.<JsArray<Operation.ToObjectReturnType.ComboAggregateFieldType.AggregatesListFieldType>>uncheckedCast(
                                aggregatesList));
            }

            @JsProperty
            void setAggregatesList(
                    JsArray<Operation.ToObjectReturnType.ComboAggregateFieldType.AggregatesListFieldType> aggregatesList);

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
                static Operation.ToObjectReturnType.CreateInputTableFieldType.GetSchemaUnionType of(
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
                    static Operation.ToObjectReturnType.CreateInputTableFieldType.KindFieldType.InMemoryKeyBackedFieldType create() {
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
                static Operation.ToObjectReturnType.CreateInputTableFieldType.KindFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getInMemoryAppendOnly();

                @JsProperty
                Operation.ToObjectReturnType.CreateInputTableFieldType.KindFieldType.InMemoryKeyBackedFieldType getInMemoryKeyBacked();

                @JsProperty
                void setInMemoryAppendOnly(Object inMemoryAppendOnly);

                @JsProperty
                void setInMemoryKeyBacked(
                        Operation.ToObjectReturnType.CreateInputTableFieldType.KindFieldType.InMemoryKeyBackedFieldType inMemoryKeyBacked);
            }

            @JsOverlay
            static Operation.ToObjectReturnType.CreateInputTableFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            Operation.ToObjectReturnType.CreateInputTableFieldType.KindFieldType getKind();

            @JsProperty
            Object getResultId();

            @JsProperty
            Operation.ToObjectReturnType.CreateInputTableFieldType.GetSchemaUnionType getSchema();

            @JsProperty
            Object getSourceTableId();

            @JsProperty
            void setKind(Operation.ToObjectReturnType.CreateInputTableFieldType.KindFieldType kind);

            @JsProperty
            void setResultId(Object resultId);

            @JsProperty
            void setSchema(
                    Operation.ToObjectReturnType.CreateInputTableFieldType.GetSchemaUnionType schema);

            @JsOverlay
            default void setSchema(String schema) {
                setSchema(
                        Js.<Operation.ToObjectReturnType.CreateInputTableFieldType.GetSchemaUnionType>uncheckedCast(
                                schema));
            }

            @JsOverlay
            default void setSchema(Uint8Array schema) {
                setSchema(
                        Js.<Operation.ToObjectReturnType.CreateInputTableFieldType.GetSchemaUnionType>uncheckedCast(
                                schema));
            }

            @JsProperty
            void setSourceTableId(Object sourceTableId);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface CrossJoinFieldType {
            @JsOverlay
            static Operation.ToObjectReturnType.CrossJoinFieldType create() {
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
                static Operation.ToObjectReturnType.DropColumnsFieldType.SourceIdFieldType create() {
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
            static Operation.ToObjectReturnType.DropColumnsFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<String> getColumnNamesList();

            @JsProperty
            Object getResultId();

            @JsProperty
            Operation.ToObjectReturnType.DropColumnsFieldType.SourceIdFieldType getSourceId();

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
                    Operation.ToObjectReturnType.DropColumnsFieldType.SourceIdFieldType sourceId);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface EmptyTableFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ResultIdFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface GetTicketUnionType {
                    @JsOverlay
                    static Operation.ToObjectReturnType.EmptyTableFieldType.ResultIdFieldType.GetTicketUnionType of(
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
                static Operation.ToObjectReturnType.EmptyTableFieldType.ResultIdFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Operation.ToObjectReturnType.EmptyTableFieldType.ResultIdFieldType.GetTicketUnionType getTicket();

                @JsProperty
                void setTicket(
                        Operation.ToObjectReturnType.EmptyTableFieldType.ResultIdFieldType.GetTicketUnionType ticket);

                @JsOverlay
                default void setTicket(String ticket) {
                    setTicket(
                            Js.<Operation.ToObjectReturnType.EmptyTableFieldType.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }

                @JsOverlay
                default void setTicket(Uint8Array ticket) {
                    setTicket(
                            Js.<Operation.ToObjectReturnType.EmptyTableFieldType.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }
            }

            @JsOverlay
            static Operation.ToObjectReturnType.EmptyTableFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            Operation.ToObjectReturnType.EmptyTableFieldType.ResultIdFieldType getResultId();

            @JsProperty
            String getSize();

            @JsProperty
            void setResultId(Operation.ToObjectReturnType.EmptyTableFieldType.ResultIdFieldType resultId);

            @JsProperty
            void setSize(String size);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ExactJoinFieldType {
            @JsOverlay
            static Operation.ToObjectReturnType.ExactJoinFieldType create() {
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
            static Operation.ToObjectReturnType.FetchTableFieldType create() {
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
                    static Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType.AndFieldType create() {
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
                            static Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType.CompareFieldType.LhsFieldType.LiteralFieldType create() {
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
                            static Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType.CompareFieldType.LhsFieldType.ReferenceFieldType create() {
                                return Js.uncheckedCast(JsPropertyMap.of());
                            }

                            @JsProperty
                            String getColumnName();

                            @JsProperty
                            void setColumnName(String columnName);
                        }

                        @JsOverlay
                        static Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType.CompareFieldType.LhsFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType.CompareFieldType.LhsFieldType.LiteralFieldType getLiteral();

                        @JsProperty
                        Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType.CompareFieldType.LhsFieldType.ReferenceFieldType getReference();

                        @JsProperty
                        void setLiteral(
                                Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType.CompareFieldType.LhsFieldType.LiteralFieldType literal);

                        @JsProperty
                        void setReference(
                                Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType.CompareFieldType.LhsFieldType.ReferenceFieldType reference);
                    }

                    @JsOverlay
                    static Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType.CompareFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    double getCaseSensitivity();

                    @JsProperty
                    Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType.CompareFieldType.LhsFieldType getLhs();

                    @JsProperty
                    double getOperation();

                    @JsProperty
                    Object getRhs();

                    @JsProperty
                    void setCaseSensitivity(double caseSensitivity);

                    @JsProperty
                    void setLhs(
                            Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType.CompareFieldType.LhsFieldType lhs);

                    @JsProperty
                    void setOperation(double operation);

                    @JsProperty
                    void setRhs(Object rhs);
                }

                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface ContainsFieldType {
                    @JsOverlay
                    static Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType.ContainsFieldType create() {
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
                    static Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType.InvokeFieldType create() {
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
                    static Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType.IsNullFieldType create() {
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
                    static Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType.MatchesFieldType create() {
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
                    static Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType.NotFieldType create() {
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
                    static Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType.OrFieldType create() {
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
                    static Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType.Pb_inFieldType create() {
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
                    static Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType.SearchFieldType create() {
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
                static Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType.AndFieldType getAnd();

                @JsProperty
                Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType.CompareFieldType getCompare();

                @JsProperty
                Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType.ContainsFieldType getContains();

                @JsProperty
                Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType.InvokeFieldType getInvoke();

                @JsProperty
                Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType.IsNullFieldType getIsNull();

                @JsProperty
                Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType.MatchesFieldType getMatches();

                @JsProperty
                Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType.NotFieldType getNot();

                @JsProperty
                Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType.OrFieldType getOr();

                @JsProperty
                Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType.Pb_inFieldType getPb_in();

                @JsProperty
                Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType.SearchFieldType getSearch();

                @JsProperty
                void setAnd(
                        Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType.AndFieldType and);

                @JsProperty
                void setCompare(
                        Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType.CompareFieldType compare);

                @JsProperty
                void setContains(
                        Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType.ContainsFieldType contains);

                @JsProperty
                void setInvoke(
                        Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType.InvokeFieldType invoke);

                @JsProperty
                void setIsNull(
                        Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType.IsNullFieldType isNull);

                @JsProperty
                void setMatches(
                        Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType.MatchesFieldType matches);

                @JsProperty
                void setNot(
                        Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType.NotFieldType not);

                @JsProperty
                void setOr(
                        Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType.OrFieldType or);

                @JsProperty
                void setPb_in(
                        Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType.Pb_inFieldType pb_in);

                @JsProperty
                void setSearch(
                        Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType.SearchFieldType search);
            }

            @JsOverlay
            static Operation.ToObjectReturnType.FilterFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType> getFiltersList();

            @JsProperty
            Object getResultId();

            @JsProperty
            Object getSourceId();

            @JsOverlay
            default void setFiltersList(
                    Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType[] filtersList) {
                setFiltersList(
                        Js.<JsArray<Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType>>uncheckedCast(
                                filtersList));
            }

            @JsProperty
            void setFiltersList(
                    JsArray<Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType> filtersList);

            @JsProperty
            void setResultId(Object resultId);

            @JsProperty
            void setSourceId(Object sourceId);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface FlattenFieldType {
            @JsOverlay
            static Operation.ToObjectReturnType.FlattenFieldType create() {
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
            static Operation.ToObjectReturnType.HeadByFieldType create() {
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
            static Operation.ToObjectReturnType.HeadFieldType create() {
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
            static Operation.ToObjectReturnType.LeftJoinFieldType create() {
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
            static Operation.ToObjectReturnType.MergeFieldType create() {
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
            static Operation.ToObjectReturnType.NaturalJoinFieldType create() {
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
                static Operation.ToObjectReturnType.RunChartDownsampleFieldType.ZoomRangeFieldType create() {
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
            static Operation.ToObjectReturnType.RunChartDownsampleFieldType create() {
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
            Operation.ToObjectReturnType.RunChartDownsampleFieldType.ZoomRangeFieldType getZoomRange();

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
                    Operation.ToObjectReturnType.RunChartDownsampleFieldType.ZoomRangeFieldType zoomRange);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SelectDistinctFieldType {
            @JsOverlay
            static Operation.ToObjectReturnType.SelectDistinctFieldType create() {
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
            static Operation.ToObjectReturnType.SnapshotFieldType create() {
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
            static Operation.ToObjectReturnType.SnapshotWhenFieldType create() {
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
                static Operation.ToObjectReturnType.SortFieldType.SortsListFieldType create() {
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
            static Operation.ToObjectReturnType.SortFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            Object getResultId();

            @JsProperty
            JsArray<Operation.ToObjectReturnType.SortFieldType.SortsListFieldType> getSortsList();

            @JsProperty
            Object getSourceId();

            @JsProperty
            void setResultId(Object resultId);

            @JsProperty
            void setSortsList(
                    JsArray<Operation.ToObjectReturnType.SortFieldType.SortsListFieldType> sortsList);

            @JsOverlay
            default void setSortsList(
                    Operation.ToObjectReturnType.SortFieldType.SortsListFieldType[] sortsList) {
                setSortsList(
                        Js.<JsArray<Operation.ToObjectReturnType.SortFieldType.SortsListFieldType>>uncheckedCast(
                                sortsList));
            }

            @JsProperty
            void setSourceId(Object sourceId);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface TimeTableFieldType {
            @JsOverlay
            static Operation.ToObjectReturnType.TimeTableFieldType create() {
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
            static Operation.ToObjectReturnType.UngroupFieldType create() {
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
            static Operation.ToObjectReturnType.UnstructuredFilterFieldType create() {
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
                                static Operation.ToObjectReturnType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.OptionsFieldType create() {
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
                                    static Operation.ToObjectReturnType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TicksFieldType create() {
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
                                    static Operation.ToObjectReturnType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TimeFieldType create() {
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
                                static Operation.ToObjectReturnType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType create() {
                                    return Js.uncheckedCast(JsPropertyMap.of());
                                }

                                @JsProperty
                                Operation.ToObjectReturnType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TicksFieldType getTicks();

                                @JsProperty
                                Operation.ToObjectReturnType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TimeFieldType getTime();

                                @JsProperty
                                void setTicks(
                                        Operation.ToObjectReturnType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TicksFieldType ticks);

                                @JsProperty
                                void setTime(
                                        Operation.ToObjectReturnType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TimeFieldType time);
                            }

                            @JsOverlay
                            static Operation.ToObjectReturnType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType create() {
                                return Js.uncheckedCast(JsPropertyMap.of());
                            }

                            @JsProperty
                            Operation.ToObjectReturnType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.OptionsFieldType getOptions();

                            @JsProperty
                            Operation.ToObjectReturnType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType getTimescale();

                            @JsProperty
                            void setOptions(
                                    Operation.ToObjectReturnType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.OptionsFieldType options);

                            @JsProperty
                            void setTimescale(
                                    Operation.ToObjectReturnType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType timescale);
                        }

                        @JsOverlay
                        static Operation.ToObjectReturnType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        Operation.ToObjectReturnType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType getEma();

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
                                Operation.ToObjectReturnType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType ema);

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
                    static Operation.ToObjectReturnType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    JsArray<String> getMatchPairsList();

                    @JsProperty
                    Operation.ToObjectReturnType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType getSpec();

                    @JsProperty
                    void setMatchPairsList(JsArray<String> matchPairsList);

                    @JsOverlay
                    default void setMatchPairsList(String[] matchPairsList) {
                        setMatchPairsList(Js.<JsArray<String>>uncheckedCast(matchPairsList));
                    }

                    @JsProperty
                    void setSpec(
                            Operation.ToObjectReturnType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType spec);
                }

                @JsOverlay
                static Operation.ToObjectReturnType.UpdateByFieldType.OperationsListFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Operation.ToObjectReturnType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType getColumn();

                @JsProperty
                void setColumn(
                        Operation.ToObjectReturnType.UpdateByFieldType.OperationsListFieldType.ColumnFieldType column);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface OptionsFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface MathContextFieldType {
                    @JsOverlay
                    static Operation.ToObjectReturnType.UpdateByFieldType.OptionsFieldType.MathContextFieldType create() {
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
                static Operation.ToObjectReturnType.UpdateByFieldType.OptionsFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                double getChunkCapacity();

                @JsProperty
                double getInitialHashTableSize();

                @JsProperty
                Operation.ToObjectReturnType.UpdateByFieldType.OptionsFieldType.MathContextFieldType getMathContext();

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
                        Operation.ToObjectReturnType.UpdateByFieldType.OptionsFieldType.MathContextFieldType mathContext);

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
            static Operation.ToObjectReturnType.UpdateByFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<String> getGroupByColumnsList();

            @JsProperty
            JsArray<Operation.ToObjectReturnType.UpdateByFieldType.OperationsListFieldType> getOperationsList();

            @JsProperty
            Operation.ToObjectReturnType.UpdateByFieldType.OptionsFieldType getOptions();

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
                    JsArray<Operation.ToObjectReturnType.UpdateByFieldType.OperationsListFieldType> operationsList);

            @JsOverlay
            default void setOperationsList(
                    Operation.ToObjectReturnType.UpdateByFieldType.OperationsListFieldType[] operationsList) {
                setOperationsList(
                        Js.<JsArray<Operation.ToObjectReturnType.UpdateByFieldType.OperationsListFieldType>>uncheckedCast(
                                operationsList));
            }

            @JsProperty
            void setOptions(Operation.ToObjectReturnType.UpdateByFieldType.OptionsFieldType options);

            @JsProperty
            void setResultId(Object resultId);

            @JsProperty
            void setSourceId(Object sourceId);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface UpdateFieldType {
            @JsOverlay
            static Operation.ToObjectReturnType.UpdateFieldType create() {
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
            static Operation.ToObjectReturnType.WhereInFieldType create() {
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
        static Operation.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Operation.ToObjectReturnType.AggregateFieldType getAggregate();

        @JsProperty
        Operation.ToObjectReturnType.AggregateAllFieldType getAggregateAll();

        @JsProperty
        Operation.ToObjectReturnType.ApplyPreviewColumnsFieldType getApplyPreviewColumns();

        @JsProperty
        Operation.ToObjectReturnType.AsOfJoinFieldType getAsOfJoin();

        @JsProperty
        Operation.ToObjectReturnType.ComboAggregateFieldType getComboAggregate();

        @JsProperty
        Operation.ToObjectReturnType.CreateInputTableFieldType getCreateInputTable();

        @JsProperty
        Operation.ToObjectReturnType.CrossJoinFieldType getCrossJoin();

        @JsProperty
        Operation.ToObjectReturnType.DropColumnsFieldType getDropColumns();

        @JsProperty
        Operation.ToObjectReturnType.EmptyTableFieldType getEmptyTable();

        @JsProperty
        Operation.ToObjectReturnType.ExactJoinFieldType getExactJoin();

        @JsProperty
        Operation.ToObjectReturnType.FetchTableFieldType getFetchTable();

        @JsProperty
        Operation.ToObjectReturnType.FilterFieldType getFilter();

        @JsProperty
        Operation.ToObjectReturnType.FlattenFieldType getFlatten();

        @JsProperty
        Operation.ToObjectReturnType.HeadFieldType getHead();

        @JsProperty
        Operation.ToObjectReturnType.HeadByFieldType getHeadBy();

        @JsProperty
        Object getLazyUpdate();

        @JsProperty
        Operation.ToObjectReturnType.LeftJoinFieldType getLeftJoin();

        @JsProperty
        Operation.ToObjectReturnType.MergeFieldType getMerge();

        @JsProperty
        Operation.ToObjectReturnType.NaturalJoinFieldType getNaturalJoin();

        @JsProperty
        Operation.ToObjectReturnType.RunChartDownsampleFieldType getRunChartDownsample();

        @JsProperty
        Object getSelect();

        @JsProperty
        Operation.ToObjectReturnType.SelectDistinctFieldType getSelectDistinct();

        @JsProperty
        Operation.ToObjectReturnType.SnapshotFieldType getSnapshot();

        @JsProperty
        Operation.ToObjectReturnType.SnapshotWhenFieldType getSnapshotWhen();

        @JsProperty
        Operation.ToObjectReturnType.SortFieldType getSort();

        @JsProperty
        Object getTail();

        @JsProperty
        Object getTailBy();

        @JsProperty
        Operation.ToObjectReturnType.TimeTableFieldType getTimeTable();

        @JsProperty
        Operation.ToObjectReturnType.UngroupFieldType getUngroup();

        @JsProperty
        Operation.ToObjectReturnType.UnstructuredFilterFieldType getUnstructuredFilter();

        @JsProperty
        Operation.ToObjectReturnType.UpdateFieldType getUpdate();

        @JsProperty
        Operation.ToObjectReturnType.UpdateByFieldType getUpdateBy();

        @JsProperty
        Object getUpdateView();

        @JsProperty
        Object getView();

        @JsProperty
        Operation.ToObjectReturnType.WhereInFieldType getWhereIn();

        @JsProperty
        void setAggregate(Operation.ToObjectReturnType.AggregateFieldType aggregate);

        @JsProperty
        void setAggregateAll(Operation.ToObjectReturnType.AggregateAllFieldType aggregateAll);

        @JsProperty
        void setApplyPreviewColumns(
                Operation.ToObjectReturnType.ApplyPreviewColumnsFieldType applyPreviewColumns);

        @JsProperty
        void setAsOfJoin(Operation.ToObjectReturnType.AsOfJoinFieldType asOfJoin);

        @JsProperty
        void setComboAggregate(Operation.ToObjectReturnType.ComboAggregateFieldType comboAggregate);

        @JsProperty
        void setCreateInputTable(
                Operation.ToObjectReturnType.CreateInputTableFieldType createInputTable);

        @JsProperty
        void setCrossJoin(Operation.ToObjectReturnType.CrossJoinFieldType crossJoin);

        @JsProperty
        void setDropColumns(Operation.ToObjectReturnType.DropColumnsFieldType dropColumns);

        @JsProperty
        void setEmptyTable(Operation.ToObjectReturnType.EmptyTableFieldType emptyTable);

        @JsProperty
        void setExactJoin(Operation.ToObjectReturnType.ExactJoinFieldType exactJoin);

        @JsProperty
        void setFetchTable(Operation.ToObjectReturnType.FetchTableFieldType fetchTable);

        @JsProperty
        void setFilter(Operation.ToObjectReturnType.FilterFieldType filter);

        @JsProperty
        void setFlatten(Operation.ToObjectReturnType.FlattenFieldType flatten);

        @JsProperty
        void setHead(Operation.ToObjectReturnType.HeadFieldType head);

        @JsProperty
        void setHeadBy(Operation.ToObjectReturnType.HeadByFieldType headBy);

        @JsProperty
        void setLazyUpdate(Object lazyUpdate);

        @JsProperty
        void setLeftJoin(Operation.ToObjectReturnType.LeftJoinFieldType leftJoin);

        @JsProperty
        void setMerge(Operation.ToObjectReturnType.MergeFieldType merge);

        @JsProperty
        void setNaturalJoin(Operation.ToObjectReturnType.NaturalJoinFieldType naturalJoin);

        @JsProperty
        void setRunChartDownsample(
                Operation.ToObjectReturnType.RunChartDownsampleFieldType runChartDownsample);

        @JsProperty
        void setSelect(Object select);

        @JsProperty
        void setSelectDistinct(Operation.ToObjectReturnType.SelectDistinctFieldType selectDistinct);

        @JsProperty
        void setSnapshot(Operation.ToObjectReturnType.SnapshotFieldType snapshot);

        @JsProperty
        void setSnapshotWhen(Operation.ToObjectReturnType.SnapshotWhenFieldType snapshotWhen);

        @JsProperty
        void setSort(Operation.ToObjectReturnType.SortFieldType sort);

        @JsProperty
        void setTail(Object tail);

        @JsProperty
        void setTailBy(Object tailBy);

        @JsProperty
        void setTimeTable(Operation.ToObjectReturnType.TimeTableFieldType timeTable);

        @JsProperty
        void setUngroup(Operation.ToObjectReturnType.UngroupFieldType ungroup);

        @JsProperty
        void setUnstructuredFilter(
                Operation.ToObjectReturnType.UnstructuredFilterFieldType unstructuredFilter);

        @JsProperty
        void setUpdate(Operation.ToObjectReturnType.UpdateFieldType update);

        @JsProperty
        void setUpdateBy(Operation.ToObjectReturnType.UpdateByFieldType updateBy);

        @JsProperty
        void setUpdateView(Object updateView);

        @JsProperty
        void setView(Object view);

        @JsProperty
        void setWhereIn(Operation.ToObjectReturnType.WhereInFieldType whereIn);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface AggregateAllFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface SpecFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface ApproximatePercentileFieldType {
                    @JsOverlay
                    static Operation.ToObjectReturnType0.AggregateAllFieldType.SpecFieldType.ApproximatePercentileFieldType create() {
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
                    static Operation.ToObjectReturnType0.AggregateAllFieldType.SpecFieldType.CountDistinctFieldType create() {
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
                    static Operation.ToObjectReturnType0.AggregateAllFieldType.SpecFieldType.DistinctFieldType create() {
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
                    static Operation.ToObjectReturnType0.AggregateAllFieldType.SpecFieldType.FormulaFieldType create() {
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
                    static Operation.ToObjectReturnType0.AggregateAllFieldType.SpecFieldType.MedianFieldType create() {
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
                    static Operation.ToObjectReturnType0.AggregateAllFieldType.SpecFieldType.PercentileFieldType create() {
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
                        static Operation.ToObjectReturnType0.AggregateAllFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        String getColumnName();

                        @JsProperty
                        void setColumnName(String columnName);
                    }

                    @JsOverlay
                    static Operation.ToObjectReturnType0.AggregateAllFieldType.SpecFieldType.SortedFirstFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    JsArray<Operation.ToObjectReturnType0.AggregateAllFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType> getColumnsList();

                    @JsOverlay
                    default void setColumnsList(
                            Operation.ToObjectReturnType0.AggregateAllFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType[] columnsList) {
                        setColumnsList(
                                Js.<JsArray<Operation.ToObjectReturnType0.AggregateAllFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType>>uncheckedCast(
                                        columnsList));
                    }

                    @JsProperty
                    void setColumnsList(
                            JsArray<Operation.ToObjectReturnType0.AggregateAllFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType> columnsList);
                }

                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface TDigestFieldType {
                    @JsOverlay
                    static Operation.ToObjectReturnType0.AggregateAllFieldType.SpecFieldType.TDigestFieldType create() {
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
                        static Operation.ToObjectReturnType0.AggregateAllFieldType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType create() {
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
                    static Operation.ToObjectReturnType0.AggregateAllFieldType.SpecFieldType.UniqueFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    Operation.ToObjectReturnType0.AggregateAllFieldType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType getNonUniqueSentinel();

                    @JsProperty
                    boolean isIncludeNulls();

                    @JsProperty
                    void setIncludeNulls(boolean includeNulls);

                    @JsProperty
                    void setNonUniqueSentinel(
                            Operation.ToObjectReturnType0.AggregateAllFieldType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType nonUniqueSentinel);
                }

                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface WeightedAvgFieldType {
                    @JsOverlay
                    static Operation.ToObjectReturnType0.AggregateAllFieldType.SpecFieldType.WeightedAvgFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    String getWeightColumn();

                    @JsProperty
                    void setWeightColumn(String weightColumn);
                }

                @JsOverlay
                static Operation.ToObjectReturnType0.AggregateAllFieldType.SpecFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getAbsSum();

                @JsProperty
                Operation.ToObjectReturnType0.AggregateAllFieldType.SpecFieldType.ApproximatePercentileFieldType getApproximatePercentile();

                @JsProperty
                Object getAvg();

                @JsProperty
                Operation.ToObjectReturnType0.AggregateAllFieldType.SpecFieldType.CountDistinctFieldType getCountDistinct();

                @JsProperty
                Operation.ToObjectReturnType0.AggregateAllFieldType.SpecFieldType.DistinctFieldType getDistinct();

                @JsProperty
                Object getFirst();

                @JsProperty
                Operation.ToObjectReturnType0.AggregateAllFieldType.SpecFieldType.FormulaFieldType getFormula();

                @JsProperty
                Object getFreeze();

                @JsProperty
                Object getGroup();

                @JsProperty
                Object getLast();

                @JsProperty
                Object getMax();

                @JsProperty
                Operation.ToObjectReturnType0.AggregateAllFieldType.SpecFieldType.MedianFieldType getMedian();

                @JsProperty
                Object getMin();

                @JsProperty
                Object getPb_var();

                @JsProperty
                Operation.ToObjectReturnType0.AggregateAllFieldType.SpecFieldType.PercentileFieldType getPercentile();

                @JsProperty
                Operation.ToObjectReturnType0.AggregateAllFieldType.SpecFieldType.SortedFirstFieldType getSortedFirst();

                @JsProperty
                Object getSortedLast();

                @JsProperty
                Object getStd();

                @JsProperty
                Object getSum();

                @JsProperty
                Operation.ToObjectReturnType0.AggregateAllFieldType.SpecFieldType.TDigestFieldType getTDigest();

                @JsProperty
                Operation.ToObjectReturnType0.AggregateAllFieldType.SpecFieldType.UniqueFieldType getUnique();

                @JsProperty
                Operation.ToObjectReturnType0.AggregateAllFieldType.SpecFieldType.WeightedAvgFieldType getWeightedAvg();

                @JsProperty
                Object getWeightedSum();

                @JsProperty
                void setAbsSum(Object absSum);

                @JsProperty
                void setApproximatePercentile(
                        Operation.ToObjectReturnType0.AggregateAllFieldType.SpecFieldType.ApproximatePercentileFieldType approximatePercentile);

                @JsProperty
                void setAvg(Object avg);

                @JsProperty
                void setCountDistinct(
                        Operation.ToObjectReturnType0.AggregateAllFieldType.SpecFieldType.CountDistinctFieldType countDistinct);

                @JsProperty
                void setDistinct(
                        Operation.ToObjectReturnType0.AggregateAllFieldType.SpecFieldType.DistinctFieldType distinct);

                @JsProperty
                void setFirst(Object first);

                @JsProperty
                void setFormula(
                        Operation.ToObjectReturnType0.AggregateAllFieldType.SpecFieldType.FormulaFieldType formula);

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
                        Operation.ToObjectReturnType0.AggregateAllFieldType.SpecFieldType.MedianFieldType median);

                @JsProperty
                void setMin(Object min);

                @JsProperty
                void setPb_var(Object pb_var);

                @JsProperty
                void setPercentile(
                        Operation.ToObjectReturnType0.AggregateAllFieldType.SpecFieldType.PercentileFieldType percentile);

                @JsProperty
                void setSortedFirst(
                        Operation.ToObjectReturnType0.AggregateAllFieldType.SpecFieldType.SortedFirstFieldType sortedFirst);

                @JsProperty
                void setSortedLast(Object sortedLast);

                @JsProperty
                void setStd(Object std);

                @JsProperty
                void setSum(Object sum);

                @JsProperty
                void setTDigest(
                        Operation.ToObjectReturnType0.AggregateAllFieldType.SpecFieldType.TDigestFieldType tDigest);

                @JsProperty
                void setUnique(
                        Operation.ToObjectReturnType0.AggregateAllFieldType.SpecFieldType.UniqueFieldType unique);

                @JsProperty
                void setWeightedAvg(
                        Operation.ToObjectReturnType0.AggregateAllFieldType.SpecFieldType.WeightedAvgFieldType weightedAvg);

                @JsProperty
                void setWeightedSum(Object weightedSum);
            }

            @JsOverlay
            static Operation.ToObjectReturnType0.AggregateAllFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<String> getGroupByColumnsList();

            @JsProperty
            Object getResultId();

            @JsProperty
            Object getSourceId();

            @JsProperty
            Operation.ToObjectReturnType0.AggregateAllFieldType.SpecFieldType getSpec();

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
            void setSpec(Operation.ToObjectReturnType0.AggregateAllFieldType.SpecFieldType spec);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface AggregateFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface AggregationsListFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface ColumnsFieldType {
                    @JsOverlay
                    static Operation.ToObjectReturnType0.AggregateFieldType.AggregationsListFieldType.ColumnsFieldType create() {
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
                    static Operation.ToObjectReturnType0.AggregateFieldType.AggregationsListFieldType.CountFieldType create() {
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
                    static Operation.ToObjectReturnType0.AggregateFieldType.AggregationsListFieldType.FirstRowKeyFieldType create() {
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
                    static Operation.ToObjectReturnType0.AggregateFieldType.AggregationsListFieldType.PartitionFieldType create() {
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
                static Operation.ToObjectReturnType0.AggregateFieldType.AggregationsListFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Operation.ToObjectReturnType0.AggregateFieldType.AggregationsListFieldType.ColumnsFieldType getColumns();

                @JsProperty
                Operation.ToObjectReturnType0.AggregateFieldType.AggregationsListFieldType.CountFieldType getCount();

                @JsProperty
                Operation.ToObjectReturnType0.AggregateFieldType.AggregationsListFieldType.FirstRowKeyFieldType getFirstRowKey();

                @JsProperty
                Object getLastRowKey();

                @JsProperty
                Operation.ToObjectReturnType0.AggregateFieldType.AggregationsListFieldType.PartitionFieldType getPartition();

                @JsProperty
                void setColumns(
                        Operation.ToObjectReturnType0.AggregateFieldType.AggregationsListFieldType.ColumnsFieldType columns);

                @JsProperty
                void setCount(
                        Operation.ToObjectReturnType0.AggregateFieldType.AggregationsListFieldType.CountFieldType count);

                @JsProperty
                void setFirstRowKey(
                        Operation.ToObjectReturnType0.AggregateFieldType.AggregationsListFieldType.FirstRowKeyFieldType firstRowKey);

                @JsProperty
                void setLastRowKey(Object lastRowKey);

                @JsProperty
                void setPartition(
                        Operation.ToObjectReturnType0.AggregateFieldType.AggregationsListFieldType.PartitionFieldType partition);
            }

            @JsOverlay
            static Operation.ToObjectReturnType0.AggregateFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<Operation.ToObjectReturnType0.AggregateFieldType.AggregationsListFieldType> getAggregationsList();

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
                    Operation.ToObjectReturnType0.AggregateFieldType.AggregationsListFieldType[] aggregationsList) {
                setAggregationsList(
                        Js.<JsArray<Operation.ToObjectReturnType0.AggregateFieldType.AggregationsListFieldType>>uncheckedCast(
                                aggregationsList));
            }

            @JsProperty
            void setAggregationsList(
                    JsArray<Operation.ToObjectReturnType0.AggregateFieldType.AggregationsListFieldType> aggregationsList);

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
            static Operation.ToObjectReturnType0.ApplyPreviewColumnsFieldType create() {
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
            static Operation.ToObjectReturnType0.AsOfJoinFieldType create() {
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
                static Operation.ToObjectReturnType0.ComboAggregateFieldType.AggregatesListFieldType create() {
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
            static Operation.ToObjectReturnType0.ComboAggregateFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<Operation.ToObjectReturnType0.ComboAggregateFieldType.AggregatesListFieldType> getAggregatesList();

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
                    Operation.ToObjectReturnType0.ComboAggregateFieldType.AggregatesListFieldType[] aggregatesList) {
                setAggregatesList(
                        Js.<JsArray<Operation.ToObjectReturnType0.ComboAggregateFieldType.AggregatesListFieldType>>uncheckedCast(
                                aggregatesList));
            }

            @JsProperty
            void setAggregatesList(
                    JsArray<Operation.ToObjectReturnType0.ComboAggregateFieldType.AggregatesListFieldType> aggregatesList);

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
                static Operation.ToObjectReturnType0.CreateInputTableFieldType.GetSchemaUnionType of(
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
                    static Operation.ToObjectReturnType0.CreateInputTableFieldType.KindFieldType.InMemoryKeyBackedFieldType create() {
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
                static Operation.ToObjectReturnType0.CreateInputTableFieldType.KindFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getInMemoryAppendOnly();

                @JsProperty
                Operation.ToObjectReturnType0.CreateInputTableFieldType.KindFieldType.InMemoryKeyBackedFieldType getInMemoryKeyBacked();

                @JsProperty
                void setInMemoryAppendOnly(Object inMemoryAppendOnly);

                @JsProperty
                void setInMemoryKeyBacked(
                        Operation.ToObjectReturnType0.CreateInputTableFieldType.KindFieldType.InMemoryKeyBackedFieldType inMemoryKeyBacked);
            }

            @JsOverlay
            static Operation.ToObjectReturnType0.CreateInputTableFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            Operation.ToObjectReturnType0.CreateInputTableFieldType.KindFieldType getKind();

            @JsProperty
            Object getResultId();

            @JsProperty
            Operation.ToObjectReturnType0.CreateInputTableFieldType.GetSchemaUnionType getSchema();

            @JsProperty
            Object getSourceTableId();

            @JsProperty
            void setKind(Operation.ToObjectReturnType0.CreateInputTableFieldType.KindFieldType kind);

            @JsProperty
            void setResultId(Object resultId);

            @JsProperty
            void setSchema(
                    Operation.ToObjectReturnType0.CreateInputTableFieldType.GetSchemaUnionType schema);

            @JsOverlay
            default void setSchema(String schema) {
                setSchema(
                        Js.<Operation.ToObjectReturnType0.CreateInputTableFieldType.GetSchemaUnionType>uncheckedCast(
                                schema));
            }

            @JsOverlay
            default void setSchema(Uint8Array schema) {
                setSchema(
                        Js.<Operation.ToObjectReturnType0.CreateInputTableFieldType.GetSchemaUnionType>uncheckedCast(
                                schema));
            }

            @JsProperty
            void setSourceTableId(Object sourceTableId);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface CrossJoinFieldType {
            @JsOverlay
            static Operation.ToObjectReturnType0.CrossJoinFieldType create() {
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
                static Operation.ToObjectReturnType0.DropColumnsFieldType.SourceIdFieldType create() {
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
            static Operation.ToObjectReturnType0.DropColumnsFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<String> getColumnNamesList();

            @JsProperty
            Object getResultId();

            @JsProperty
            Operation.ToObjectReturnType0.DropColumnsFieldType.SourceIdFieldType getSourceId();

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
                    Operation.ToObjectReturnType0.DropColumnsFieldType.SourceIdFieldType sourceId);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface EmptyTableFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ResultIdFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface GetTicketUnionType {
                    @JsOverlay
                    static Operation.ToObjectReturnType0.EmptyTableFieldType.ResultIdFieldType.GetTicketUnionType of(
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
                static Operation.ToObjectReturnType0.EmptyTableFieldType.ResultIdFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Operation.ToObjectReturnType0.EmptyTableFieldType.ResultIdFieldType.GetTicketUnionType getTicket();

                @JsProperty
                void setTicket(
                        Operation.ToObjectReturnType0.EmptyTableFieldType.ResultIdFieldType.GetTicketUnionType ticket);

                @JsOverlay
                default void setTicket(String ticket) {
                    setTicket(
                            Js.<Operation.ToObjectReturnType0.EmptyTableFieldType.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }

                @JsOverlay
                default void setTicket(Uint8Array ticket) {
                    setTicket(
                            Js.<Operation.ToObjectReturnType0.EmptyTableFieldType.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                    ticket));
                }
            }

            @JsOverlay
            static Operation.ToObjectReturnType0.EmptyTableFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            Operation.ToObjectReturnType0.EmptyTableFieldType.ResultIdFieldType getResultId();

            @JsProperty
            String getSize();

            @JsProperty
            void setResultId(
                    Operation.ToObjectReturnType0.EmptyTableFieldType.ResultIdFieldType resultId);

            @JsProperty
            void setSize(String size);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ExactJoinFieldType {
            @JsOverlay
            static Operation.ToObjectReturnType0.ExactJoinFieldType create() {
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
            static Operation.ToObjectReturnType0.FetchTableFieldType create() {
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
                    static Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType.AndFieldType create() {
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
                            static Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType.CompareFieldType.LhsFieldType.LiteralFieldType create() {
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
                            static Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType.CompareFieldType.LhsFieldType.ReferenceFieldType create() {
                                return Js.uncheckedCast(JsPropertyMap.of());
                            }

                            @JsProperty
                            String getColumnName();

                            @JsProperty
                            void setColumnName(String columnName);
                        }

                        @JsOverlay
                        static Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType.CompareFieldType.LhsFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType.CompareFieldType.LhsFieldType.LiteralFieldType getLiteral();

                        @JsProperty
                        Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType.CompareFieldType.LhsFieldType.ReferenceFieldType getReference();

                        @JsProperty
                        void setLiteral(
                                Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType.CompareFieldType.LhsFieldType.LiteralFieldType literal);

                        @JsProperty
                        void setReference(
                                Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType.CompareFieldType.LhsFieldType.ReferenceFieldType reference);
                    }

                    @JsOverlay
                    static Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType.CompareFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    double getCaseSensitivity();

                    @JsProperty
                    Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType.CompareFieldType.LhsFieldType getLhs();

                    @JsProperty
                    double getOperation();

                    @JsProperty
                    Object getRhs();

                    @JsProperty
                    void setCaseSensitivity(double caseSensitivity);

                    @JsProperty
                    void setLhs(
                            Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType.CompareFieldType.LhsFieldType lhs);

                    @JsProperty
                    void setOperation(double operation);

                    @JsProperty
                    void setRhs(Object rhs);
                }

                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface ContainsFieldType {
                    @JsOverlay
                    static Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType.ContainsFieldType create() {
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
                    static Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType.InvokeFieldType create() {
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
                    static Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType.IsNullFieldType create() {
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
                    static Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType.MatchesFieldType create() {
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
                    static Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType.NotFieldType create() {
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
                    static Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType.OrFieldType create() {
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
                    static Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType.Pb_inFieldType create() {
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
                    static Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType.SearchFieldType create() {
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
                static Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType.AndFieldType getAnd();

                @JsProperty
                Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType.CompareFieldType getCompare();

                @JsProperty
                Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType.ContainsFieldType getContains();

                @JsProperty
                Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType.InvokeFieldType getInvoke();

                @JsProperty
                Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType.IsNullFieldType getIsNull();

                @JsProperty
                Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType.MatchesFieldType getMatches();

                @JsProperty
                Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType.NotFieldType getNot();

                @JsProperty
                Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType.OrFieldType getOr();

                @JsProperty
                Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType.Pb_inFieldType getPb_in();

                @JsProperty
                Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType.SearchFieldType getSearch();

                @JsProperty
                void setAnd(
                        Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType.AndFieldType and);

                @JsProperty
                void setCompare(
                        Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType.CompareFieldType compare);

                @JsProperty
                void setContains(
                        Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType.ContainsFieldType contains);

                @JsProperty
                void setInvoke(
                        Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType.InvokeFieldType invoke);

                @JsProperty
                void setIsNull(
                        Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType.IsNullFieldType isNull);

                @JsProperty
                void setMatches(
                        Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType.MatchesFieldType matches);

                @JsProperty
                void setNot(
                        Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType.NotFieldType not);

                @JsProperty
                void setOr(
                        Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType.OrFieldType or);

                @JsProperty
                void setPb_in(
                        Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType.Pb_inFieldType pb_in);

                @JsProperty
                void setSearch(
                        Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType.SearchFieldType search);
            }

            @JsOverlay
            static Operation.ToObjectReturnType0.FilterFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType> getFiltersList();

            @JsProperty
            Object getResultId();

            @JsProperty
            Object getSourceId();

            @JsOverlay
            default void setFiltersList(
                    Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType[] filtersList) {
                setFiltersList(
                        Js.<JsArray<Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType>>uncheckedCast(
                                filtersList));
            }

            @JsProperty
            void setFiltersList(
                    JsArray<Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType> filtersList);

            @JsProperty
            void setResultId(Object resultId);

            @JsProperty
            void setSourceId(Object sourceId);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface FlattenFieldType {
            @JsOverlay
            static Operation.ToObjectReturnType0.FlattenFieldType create() {
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
            static Operation.ToObjectReturnType0.HeadByFieldType create() {
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
            static Operation.ToObjectReturnType0.HeadFieldType create() {
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
            static Operation.ToObjectReturnType0.LeftJoinFieldType create() {
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
            static Operation.ToObjectReturnType0.MergeFieldType create() {
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
            static Operation.ToObjectReturnType0.NaturalJoinFieldType create() {
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
                static Operation.ToObjectReturnType0.RunChartDownsampleFieldType.ZoomRangeFieldType create() {
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
            static Operation.ToObjectReturnType0.RunChartDownsampleFieldType create() {
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
            Operation.ToObjectReturnType0.RunChartDownsampleFieldType.ZoomRangeFieldType getZoomRange();

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
                    Operation.ToObjectReturnType0.RunChartDownsampleFieldType.ZoomRangeFieldType zoomRange);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SelectDistinctFieldType {
            @JsOverlay
            static Operation.ToObjectReturnType0.SelectDistinctFieldType create() {
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
            static Operation.ToObjectReturnType0.SnapshotFieldType create() {
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
            static Operation.ToObjectReturnType0.SnapshotWhenFieldType create() {
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
                static Operation.ToObjectReturnType0.SortFieldType.SortsListFieldType create() {
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
            static Operation.ToObjectReturnType0.SortFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            Object getResultId();

            @JsProperty
            JsArray<Operation.ToObjectReturnType0.SortFieldType.SortsListFieldType> getSortsList();

            @JsProperty
            Object getSourceId();

            @JsProperty
            void setResultId(Object resultId);

            @JsProperty
            void setSortsList(
                    JsArray<Operation.ToObjectReturnType0.SortFieldType.SortsListFieldType> sortsList);

            @JsOverlay
            default void setSortsList(
                    Operation.ToObjectReturnType0.SortFieldType.SortsListFieldType[] sortsList) {
                setSortsList(
                        Js.<JsArray<Operation.ToObjectReturnType0.SortFieldType.SortsListFieldType>>uncheckedCast(
                                sortsList));
            }

            @JsProperty
            void setSourceId(Object sourceId);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface TimeTableFieldType {
            @JsOverlay
            static Operation.ToObjectReturnType0.TimeTableFieldType create() {
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
            static Operation.ToObjectReturnType0.UngroupFieldType create() {
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
            static Operation.ToObjectReturnType0.UnstructuredFilterFieldType create() {
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
                                static Operation.ToObjectReturnType0.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.OptionsFieldType create() {
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
                                    static Operation.ToObjectReturnType0.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TicksFieldType create() {
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
                                    static Operation.ToObjectReturnType0.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TimeFieldType create() {
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
                                static Operation.ToObjectReturnType0.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType create() {
                                    return Js.uncheckedCast(JsPropertyMap.of());
                                }

                                @JsProperty
                                Operation.ToObjectReturnType0.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TicksFieldType getTicks();

                                @JsProperty
                                Operation.ToObjectReturnType0.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TimeFieldType getTime();

                                @JsProperty
                                void setTicks(
                                        Operation.ToObjectReturnType0.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TicksFieldType ticks);

                                @JsProperty
                                void setTime(
                                        Operation.ToObjectReturnType0.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType.TimeFieldType time);
                            }

                            @JsOverlay
                            static Operation.ToObjectReturnType0.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType create() {
                                return Js.uncheckedCast(JsPropertyMap.of());
                            }

                            @JsProperty
                            Operation.ToObjectReturnType0.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.OptionsFieldType getOptions();

                            @JsProperty
                            Operation.ToObjectReturnType0.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType getTimescale();

                            @JsProperty
                            void setOptions(
                                    Operation.ToObjectReturnType0.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.OptionsFieldType options);

                            @JsProperty
                            void setTimescale(
                                    Operation.ToObjectReturnType0.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType.TimescaleFieldType timescale);
                        }

                        @JsOverlay
                        static Operation.ToObjectReturnType0.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        Operation.ToObjectReturnType0.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType getEma();

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
                                Operation.ToObjectReturnType0.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType.EmaFieldType ema);

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
                    static Operation.ToObjectReturnType0.UpdateByFieldType.OperationsListFieldType.ColumnFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    JsArray<String> getMatchPairsList();

                    @JsProperty
                    Operation.ToObjectReturnType0.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType getSpec();

                    @JsProperty
                    void setMatchPairsList(JsArray<String> matchPairsList);

                    @JsOverlay
                    default void setMatchPairsList(String[] matchPairsList) {
                        setMatchPairsList(Js.<JsArray<String>>uncheckedCast(matchPairsList));
                    }

                    @JsProperty
                    void setSpec(
                            Operation.ToObjectReturnType0.UpdateByFieldType.OperationsListFieldType.ColumnFieldType.SpecFieldType spec);
                }

                @JsOverlay
                static Operation.ToObjectReturnType0.UpdateByFieldType.OperationsListFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Operation.ToObjectReturnType0.UpdateByFieldType.OperationsListFieldType.ColumnFieldType getColumn();

                @JsProperty
                void setColumn(
                        Operation.ToObjectReturnType0.UpdateByFieldType.OperationsListFieldType.ColumnFieldType column);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface OptionsFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface MathContextFieldType {
                    @JsOverlay
                    static Operation.ToObjectReturnType0.UpdateByFieldType.OptionsFieldType.MathContextFieldType create() {
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
                static Operation.ToObjectReturnType0.UpdateByFieldType.OptionsFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                double getChunkCapacity();

                @JsProperty
                double getInitialHashTableSize();

                @JsProperty
                Operation.ToObjectReturnType0.UpdateByFieldType.OptionsFieldType.MathContextFieldType getMathContext();

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
                        Operation.ToObjectReturnType0.UpdateByFieldType.OptionsFieldType.MathContextFieldType mathContext);

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
            static Operation.ToObjectReturnType0.UpdateByFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<String> getGroupByColumnsList();

            @JsProperty
            JsArray<Operation.ToObjectReturnType0.UpdateByFieldType.OperationsListFieldType> getOperationsList();

            @JsProperty
            Operation.ToObjectReturnType0.UpdateByFieldType.OptionsFieldType getOptions();

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
                    JsArray<Operation.ToObjectReturnType0.UpdateByFieldType.OperationsListFieldType> operationsList);

            @JsOverlay
            default void setOperationsList(
                    Operation.ToObjectReturnType0.UpdateByFieldType.OperationsListFieldType[] operationsList) {
                setOperationsList(
                        Js.<JsArray<Operation.ToObjectReturnType0.UpdateByFieldType.OperationsListFieldType>>uncheckedCast(
                                operationsList));
            }

            @JsProperty
            void setOptions(Operation.ToObjectReturnType0.UpdateByFieldType.OptionsFieldType options);

            @JsProperty
            void setResultId(Object resultId);

            @JsProperty
            void setSourceId(Object sourceId);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface UpdateFieldType {
            @JsOverlay
            static Operation.ToObjectReturnType0.UpdateFieldType create() {
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
            static Operation.ToObjectReturnType0.WhereInFieldType create() {
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
        static Operation.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Operation.ToObjectReturnType0.AggregateFieldType getAggregate();

        @JsProperty
        Operation.ToObjectReturnType0.AggregateAllFieldType getAggregateAll();

        @JsProperty
        Operation.ToObjectReturnType0.ApplyPreviewColumnsFieldType getApplyPreviewColumns();

        @JsProperty
        Operation.ToObjectReturnType0.AsOfJoinFieldType getAsOfJoin();

        @JsProperty
        Operation.ToObjectReturnType0.ComboAggregateFieldType getComboAggregate();

        @JsProperty
        Operation.ToObjectReturnType0.CreateInputTableFieldType getCreateInputTable();

        @JsProperty
        Operation.ToObjectReturnType0.CrossJoinFieldType getCrossJoin();

        @JsProperty
        Operation.ToObjectReturnType0.DropColumnsFieldType getDropColumns();

        @JsProperty
        Operation.ToObjectReturnType0.EmptyTableFieldType getEmptyTable();

        @JsProperty
        Operation.ToObjectReturnType0.ExactJoinFieldType getExactJoin();

        @JsProperty
        Operation.ToObjectReturnType0.FetchTableFieldType getFetchTable();

        @JsProperty
        Operation.ToObjectReturnType0.FilterFieldType getFilter();

        @JsProperty
        Operation.ToObjectReturnType0.FlattenFieldType getFlatten();

        @JsProperty
        Operation.ToObjectReturnType0.HeadFieldType getHead();

        @JsProperty
        Operation.ToObjectReturnType0.HeadByFieldType getHeadBy();

        @JsProperty
        Object getLazyUpdate();

        @JsProperty
        Operation.ToObjectReturnType0.LeftJoinFieldType getLeftJoin();

        @JsProperty
        Operation.ToObjectReturnType0.MergeFieldType getMerge();

        @JsProperty
        Operation.ToObjectReturnType0.NaturalJoinFieldType getNaturalJoin();

        @JsProperty
        Operation.ToObjectReturnType0.RunChartDownsampleFieldType getRunChartDownsample();

        @JsProperty
        Object getSelect();

        @JsProperty
        Operation.ToObjectReturnType0.SelectDistinctFieldType getSelectDistinct();

        @JsProperty
        Operation.ToObjectReturnType0.SnapshotFieldType getSnapshot();

        @JsProperty
        Operation.ToObjectReturnType0.SnapshotWhenFieldType getSnapshotWhen();

        @JsProperty
        Operation.ToObjectReturnType0.SortFieldType getSort();

        @JsProperty
        Object getTail();

        @JsProperty
        Object getTailBy();

        @JsProperty
        Operation.ToObjectReturnType0.TimeTableFieldType getTimeTable();

        @JsProperty
        Operation.ToObjectReturnType0.UngroupFieldType getUngroup();

        @JsProperty
        Operation.ToObjectReturnType0.UnstructuredFilterFieldType getUnstructuredFilter();

        @JsProperty
        Operation.ToObjectReturnType0.UpdateFieldType getUpdate();

        @JsProperty
        Operation.ToObjectReturnType0.UpdateByFieldType getUpdateBy();

        @JsProperty
        Object getUpdateView();

        @JsProperty
        Object getView();

        @JsProperty
        Operation.ToObjectReturnType0.WhereInFieldType getWhereIn();

        @JsProperty
        void setAggregate(Operation.ToObjectReturnType0.AggregateFieldType aggregate);

        @JsProperty
        void setAggregateAll(Operation.ToObjectReturnType0.AggregateAllFieldType aggregateAll);

        @JsProperty
        void setApplyPreviewColumns(
                Operation.ToObjectReturnType0.ApplyPreviewColumnsFieldType applyPreviewColumns);

        @JsProperty
        void setAsOfJoin(Operation.ToObjectReturnType0.AsOfJoinFieldType asOfJoin);

        @JsProperty
        void setComboAggregate(Operation.ToObjectReturnType0.ComboAggregateFieldType comboAggregate);

        @JsProperty
        void setCreateInputTable(
                Operation.ToObjectReturnType0.CreateInputTableFieldType createInputTable);

        @JsProperty
        void setCrossJoin(Operation.ToObjectReturnType0.CrossJoinFieldType crossJoin);

        @JsProperty
        void setDropColumns(Operation.ToObjectReturnType0.DropColumnsFieldType dropColumns);

        @JsProperty
        void setEmptyTable(Operation.ToObjectReturnType0.EmptyTableFieldType emptyTable);

        @JsProperty
        void setExactJoin(Operation.ToObjectReturnType0.ExactJoinFieldType exactJoin);

        @JsProperty
        void setFetchTable(Operation.ToObjectReturnType0.FetchTableFieldType fetchTable);

        @JsProperty
        void setFilter(Operation.ToObjectReturnType0.FilterFieldType filter);

        @JsProperty
        void setFlatten(Operation.ToObjectReturnType0.FlattenFieldType flatten);

        @JsProperty
        void setHead(Operation.ToObjectReturnType0.HeadFieldType head);

        @JsProperty
        void setHeadBy(Operation.ToObjectReturnType0.HeadByFieldType headBy);

        @JsProperty
        void setLazyUpdate(Object lazyUpdate);

        @JsProperty
        void setLeftJoin(Operation.ToObjectReturnType0.LeftJoinFieldType leftJoin);

        @JsProperty
        void setMerge(Operation.ToObjectReturnType0.MergeFieldType merge);

        @JsProperty
        void setNaturalJoin(Operation.ToObjectReturnType0.NaturalJoinFieldType naturalJoin);

        @JsProperty
        void setRunChartDownsample(
                Operation.ToObjectReturnType0.RunChartDownsampleFieldType runChartDownsample);

        @JsProperty
        void setSelect(Object select);

        @JsProperty
        void setSelectDistinct(Operation.ToObjectReturnType0.SelectDistinctFieldType selectDistinct);

        @JsProperty
        void setSnapshot(Operation.ToObjectReturnType0.SnapshotFieldType snapshot);

        @JsProperty
        void setSnapshotWhen(Operation.ToObjectReturnType0.SnapshotWhenFieldType snapshotWhen);

        @JsProperty
        void setSort(Operation.ToObjectReturnType0.SortFieldType sort);

        @JsProperty
        void setTail(Object tail);

        @JsProperty
        void setTailBy(Object tailBy);

        @JsProperty
        void setTimeTable(Operation.ToObjectReturnType0.TimeTableFieldType timeTable);

        @JsProperty
        void setUngroup(Operation.ToObjectReturnType0.UngroupFieldType ungroup);

        @JsProperty
        void setUnstructuredFilter(
                Operation.ToObjectReturnType0.UnstructuredFilterFieldType unstructuredFilter);

        @JsProperty
        void setUpdate(Operation.ToObjectReturnType0.UpdateFieldType update);

        @JsProperty
        void setUpdateBy(Operation.ToObjectReturnType0.UpdateByFieldType updateBy);

        @JsProperty
        void setUpdateView(Object updateView);

        @JsProperty
        void setView(Object view);

        @JsProperty
        void setWhereIn(Operation.ToObjectReturnType0.WhereInFieldType whereIn);
    }

    public static native Operation deserializeBinary(Uint8Array bytes);

    public static native Operation deserializeBinaryFromReader(Operation message, Object reader);

    public static native void serializeBinaryToWriter(Operation message, Object writer);

    public static native Operation.ToObjectReturnType toObject(
            boolean includeInstance, Operation msg);

    public native void clearAggregate();

    public native void clearAggregateAll();

    public native void clearApplyPreviewColumns();

    public native void clearAsOfJoin();

    public native void clearComboAggregate();

    public native void clearCreateInputTable();

    public native void clearCrossJoin();

    public native void clearDropColumns();

    public native void clearEmptyTable();

    public native void clearExactJoin();

    public native void clearFetchTable();

    public native void clearFilter();

    public native void clearFlatten();

    public native void clearHead();

    public native void clearHeadBy();

    public native void clearLazyUpdate();

    public native void clearLeftJoin();

    public native void clearMerge();

    public native void clearNaturalJoin();

    public native void clearRunChartDownsample();

    public native void clearSelect();

    public native void clearSelectDistinct();

    public native void clearSnapshot();

    public native void clearSnapshotWhen();

    public native void clearSort();

    public native void clearTail();

    public native void clearTailBy();

    public native void clearTimeTable();

    public native void clearUngroup();

    public native void clearUnstructuredFilter();

    public native void clearUpdate();

    public native void clearUpdateBy();

    public native void clearUpdateView();

    public native void clearView();

    public native void clearWhereIn();

    public native AggregateRequest getAggregate();

    public native AggregateAllRequest getAggregateAll();

    public native ApplyPreviewColumnsRequest getApplyPreviewColumns();

    public native AsOfJoinTablesRequest getAsOfJoin();

    public native ComboAggregateRequest getComboAggregate();

    public native CreateInputTableRequest getCreateInputTable();

    public native CrossJoinTablesRequest getCrossJoin();

    public native DropColumnsRequest getDropColumns();

    public native EmptyTableRequest getEmptyTable();

    public native ExactJoinTablesRequest getExactJoin();

    public native FetchTableRequest getFetchTable();

    public native FilterTableRequest getFilter();

    public native FlattenRequest getFlatten();

    public native HeadOrTailRequest getHead();

    public native HeadOrTailByRequest getHeadBy();

    public native SelectOrUpdateRequest getLazyUpdate();

    public native LeftJoinTablesRequest getLeftJoin();

    public native MergeTablesRequest getMerge();

    public native NaturalJoinTablesRequest getNaturalJoin();

    public native int getOpCase();

    public native RunChartDownsampleRequest getRunChartDownsample();

    public native SelectOrUpdateRequest getSelect();

    public native SelectDistinctRequest getSelectDistinct();

    public native SnapshotTableRequest getSnapshot();

    public native SnapshotWhenTableRequest getSnapshotWhen();

    public native SortTableRequest getSort();

    public native HeadOrTailRequest getTail();

    public native HeadOrTailByRequest getTailBy();

    public native TimeTableRequest getTimeTable();

    public native UngroupRequest getUngroup();

    public native UnstructuredFilterTableRequest getUnstructuredFilter();

    public native SelectOrUpdateRequest getUpdate();

    public native UpdateByRequest getUpdateBy();

    public native SelectOrUpdateRequest getUpdateView();

    public native SelectOrUpdateRequest getView();

    public native WhereInRequest getWhereIn();

    public native boolean hasAggregate();

    public native boolean hasAggregateAll();

    public native boolean hasApplyPreviewColumns();

    public native boolean hasAsOfJoin();

    public native boolean hasComboAggregate();

    public native boolean hasCreateInputTable();

    public native boolean hasCrossJoin();

    public native boolean hasDropColumns();

    public native boolean hasEmptyTable();

    public native boolean hasExactJoin();

    public native boolean hasFetchTable();

    public native boolean hasFilter();

    public native boolean hasFlatten();

    public native boolean hasHead();

    public native boolean hasHeadBy();

    public native boolean hasLazyUpdate();

    public native boolean hasLeftJoin();

    public native boolean hasMerge();

    public native boolean hasNaturalJoin();

    public native boolean hasRunChartDownsample();

    public native boolean hasSelect();

    public native boolean hasSelectDistinct();

    public native boolean hasSnapshot();

    public native boolean hasSnapshotWhen();

    public native boolean hasSort();

    public native boolean hasTail();

    public native boolean hasTailBy();

    public native boolean hasTimeTable();

    public native boolean hasUngroup();

    public native boolean hasUnstructuredFilter();

    public native boolean hasUpdate();

    public native boolean hasUpdateBy();

    public native boolean hasUpdateView();

    public native boolean hasView();

    public native boolean hasWhereIn();

    public native Uint8Array serializeBinary();

    public native void setAggregate();

    public native void setAggregate(AggregateRequest value);

    public native void setAggregateAll();

    public native void setAggregateAll(AggregateAllRequest value);

    public native void setApplyPreviewColumns();

    public native void setApplyPreviewColumns(ApplyPreviewColumnsRequest value);

    public native void setAsOfJoin();

    public native void setAsOfJoin(AsOfJoinTablesRequest value);

    public native void setComboAggregate();

    public native void setComboAggregate(ComboAggregateRequest value);

    public native void setCreateInputTable();

    public native void setCreateInputTable(CreateInputTableRequest value);

    public native void setCrossJoin();

    public native void setCrossJoin(CrossJoinTablesRequest value);

    public native void setDropColumns();

    public native void setDropColumns(DropColumnsRequest value);

    public native void setEmptyTable();

    public native void setEmptyTable(EmptyTableRequest value);

    public native void setExactJoin();

    public native void setExactJoin(ExactJoinTablesRequest value);

    public native void setFetchTable();

    public native void setFetchTable(FetchTableRequest value);

    public native void setFilter();

    public native void setFilter(FilterTableRequest value);

    public native void setFlatten();

    public native void setFlatten(FlattenRequest value);

    public native void setHead();

    public native void setHead(HeadOrTailRequest value);

    public native void setHeadBy();

    public native void setHeadBy(HeadOrTailByRequest value);

    public native void setLazyUpdate();

    public native void setLazyUpdate(SelectOrUpdateRequest value);

    public native void setLeftJoin();

    public native void setLeftJoin(LeftJoinTablesRequest value);

    public native void setMerge();

    public native void setMerge(MergeTablesRequest value);

    public native void setNaturalJoin();

    public native void setNaturalJoin(NaturalJoinTablesRequest value);

    public native void setRunChartDownsample();

    public native void setRunChartDownsample(RunChartDownsampleRequest value);

    public native void setSelect();

    public native void setSelect(SelectOrUpdateRequest value);

    public native void setSelectDistinct();

    public native void setSelectDistinct(SelectDistinctRequest value);

    public native void setSnapshot();

    public native void setSnapshot(SnapshotTableRequest value);

    public native void setSnapshotWhen();

    public native void setSnapshotWhen(SnapshotWhenTableRequest value);

    public native void setSort();

    public native void setSort(SortTableRequest value);

    public native void setTail();

    public native void setTail(HeadOrTailRequest value);

    public native void setTailBy();

    public native void setTailBy(HeadOrTailByRequest value);

    public native void setTimeTable();

    public native void setTimeTable(TimeTableRequest value);

    public native void setUngroup();

    public native void setUngroup(UngroupRequest value);

    public native void setUnstructuredFilter();

    public native void setUnstructuredFilter(UnstructuredFilterTableRequest value);

    public native void setUpdate();

    public native void setUpdate(SelectOrUpdateRequest value);

    public native void setUpdateBy();

    public native void setUpdateBy(UpdateByRequest value);

    public native void setUpdateView();

    public native void setUpdateView(SelectOrUpdateRequest value);

    public native void setView();

    public native void setView(SelectOrUpdateRequest value);

    public native void setWhereIn();

    public native void setWhereIn(WhereInRequest value);

    public native Operation.ToObjectReturnType0 toObject();

    public native Operation.ToObjectReturnType0 toObject(boolean includeInstance);
}
