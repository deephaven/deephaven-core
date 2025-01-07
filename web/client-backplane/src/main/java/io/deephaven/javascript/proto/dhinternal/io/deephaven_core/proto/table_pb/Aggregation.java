//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.aggregation.AggregationColumns;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.aggregation.AggregationCount;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.aggregation.AggregationPartition;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.aggregation.AggregationRowKey;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.table_pb.Aggregation",
        namespace = JsPackage.GLOBAL)
public class Aggregation {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ColumnsFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface SpecFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface ApproximatePercentileFieldType {
                    @JsOverlay
                    static Aggregation.ToObjectReturnType.ColumnsFieldType.SpecFieldType.ApproximatePercentileFieldType create() {
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
                    static Aggregation.ToObjectReturnType.ColumnsFieldType.SpecFieldType.CountDistinctFieldType create() {
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
                    static Aggregation.ToObjectReturnType.ColumnsFieldType.SpecFieldType.DistinctFieldType create() {
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
                    static Aggregation.ToObjectReturnType.ColumnsFieldType.SpecFieldType.FormulaFieldType create() {
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
                    static Aggregation.ToObjectReturnType.ColumnsFieldType.SpecFieldType.MedianFieldType create() {
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
                    static Aggregation.ToObjectReturnType.ColumnsFieldType.SpecFieldType.PercentileFieldType create() {
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
                        static Aggregation.ToObjectReturnType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        String getColumnName();

                        @JsProperty
                        void setColumnName(String columnName);
                    }

                    @JsOverlay
                    static Aggregation.ToObjectReturnType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    JsArray<Aggregation.ToObjectReturnType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType> getColumnsList();

                    @JsOverlay
                    default void setColumnsList(
                            Aggregation.ToObjectReturnType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType[] columnsList) {
                        setColumnsList(
                                Js.<JsArray<Aggregation.ToObjectReturnType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType>>uncheckedCast(
                                        columnsList));
                    }

                    @JsProperty
                    void setColumnsList(
                            JsArray<Aggregation.ToObjectReturnType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType> columnsList);
                }

                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface TDigestFieldType {
                    @JsOverlay
                    static Aggregation.ToObjectReturnType.ColumnsFieldType.SpecFieldType.TDigestFieldType create() {
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
                        static Aggregation.ToObjectReturnType.ColumnsFieldType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType create() {
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
                    static Aggregation.ToObjectReturnType.ColumnsFieldType.SpecFieldType.UniqueFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    Aggregation.ToObjectReturnType.ColumnsFieldType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType getNonUniqueSentinel();

                    @JsProperty
                    boolean isIncludeNulls();

                    @JsProperty
                    void setIncludeNulls(boolean includeNulls);

                    @JsProperty
                    void setNonUniqueSentinel(
                            Aggregation.ToObjectReturnType.ColumnsFieldType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType nonUniqueSentinel);
                }

                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface WeightedAvgFieldType {
                    @JsOverlay
                    static Aggregation.ToObjectReturnType.ColumnsFieldType.SpecFieldType.WeightedAvgFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    String getWeightColumn();

                    @JsProperty
                    void setWeightColumn(String weightColumn);
                }

                @JsOverlay
                static Aggregation.ToObjectReturnType.ColumnsFieldType.SpecFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getAbsSum();

                @JsProperty
                Aggregation.ToObjectReturnType.ColumnsFieldType.SpecFieldType.ApproximatePercentileFieldType getApproximatePercentile();

                @JsProperty
                Object getAvg();

                @JsProperty
                Aggregation.ToObjectReturnType.ColumnsFieldType.SpecFieldType.CountDistinctFieldType getCountDistinct();

                @JsProperty
                Aggregation.ToObjectReturnType.ColumnsFieldType.SpecFieldType.DistinctFieldType getDistinct();

                @JsProperty
                Object getFirst();

                @JsProperty
                Aggregation.ToObjectReturnType.ColumnsFieldType.SpecFieldType.FormulaFieldType getFormula();

                @JsProperty
                Object getFreeze();

                @JsProperty
                Object getGroup();

                @JsProperty
                Object getLast();

                @JsProperty
                Object getMax();

                @JsProperty
                Aggregation.ToObjectReturnType.ColumnsFieldType.SpecFieldType.MedianFieldType getMedian();

                @JsProperty
                Object getMin();

                @JsProperty
                Object getPb_var();

                @JsProperty
                Aggregation.ToObjectReturnType.ColumnsFieldType.SpecFieldType.PercentileFieldType getPercentile();

                @JsProperty
                Aggregation.ToObjectReturnType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType getSortedFirst();

                @JsProperty
                Object getSortedLast();

                @JsProperty
                Object getStd();

                @JsProperty
                Object getSum();

                @JsProperty
                Aggregation.ToObjectReturnType.ColumnsFieldType.SpecFieldType.TDigestFieldType getTDigest();

                @JsProperty
                Aggregation.ToObjectReturnType.ColumnsFieldType.SpecFieldType.UniqueFieldType getUnique();

                @JsProperty
                Aggregation.ToObjectReturnType.ColumnsFieldType.SpecFieldType.WeightedAvgFieldType getWeightedAvg();

                @JsProperty
                Object getWeightedSum();

                @JsProperty
                void setAbsSum(Object absSum);

                @JsProperty
                void setApproximatePercentile(
                        Aggregation.ToObjectReturnType.ColumnsFieldType.SpecFieldType.ApproximatePercentileFieldType approximatePercentile);

                @JsProperty
                void setAvg(Object avg);

                @JsProperty
                void setCountDistinct(
                        Aggregation.ToObjectReturnType.ColumnsFieldType.SpecFieldType.CountDistinctFieldType countDistinct);

                @JsProperty
                void setDistinct(
                        Aggregation.ToObjectReturnType.ColumnsFieldType.SpecFieldType.DistinctFieldType distinct);

                @JsProperty
                void setFirst(Object first);

                @JsProperty
                void setFormula(
                        Aggregation.ToObjectReturnType.ColumnsFieldType.SpecFieldType.FormulaFieldType formula);

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
                        Aggregation.ToObjectReturnType.ColumnsFieldType.SpecFieldType.MedianFieldType median);

                @JsProperty
                void setMin(Object min);

                @JsProperty
                void setPb_var(Object pb_var);

                @JsProperty
                void setPercentile(
                        Aggregation.ToObjectReturnType.ColumnsFieldType.SpecFieldType.PercentileFieldType percentile);

                @JsProperty
                void setSortedFirst(
                        Aggregation.ToObjectReturnType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType sortedFirst);

                @JsProperty
                void setSortedLast(Object sortedLast);

                @JsProperty
                void setStd(Object std);

                @JsProperty
                void setSum(Object sum);

                @JsProperty
                void setTDigest(
                        Aggregation.ToObjectReturnType.ColumnsFieldType.SpecFieldType.TDigestFieldType tDigest);

                @JsProperty
                void setUnique(
                        Aggregation.ToObjectReturnType.ColumnsFieldType.SpecFieldType.UniqueFieldType unique);

                @JsProperty
                void setWeightedAvg(
                        Aggregation.ToObjectReturnType.ColumnsFieldType.SpecFieldType.WeightedAvgFieldType weightedAvg);

                @JsProperty
                void setWeightedSum(Object weightedSum);
            }

            @JsOverlay
            static Aggregation.ToObjectReturnType.ColumnsFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<String> getMatchPairsList();

            @JsProperty
            Aggregation.ToObjectReturnType.ColumnsFieldType.SpecFieldType getSpec();

            @JsProperty
            void setMatchPairsList(JsArray<String> matchPairsList);

            @JsOverlay
            default void setMatchPairsList(String[] matchPairsList) {
                setMatchPairsList(Js.<JsArray<String>>uncheckedCast(matchPairsList));
            }

            @JsProperty
            void setSpec(Aggregation.ToObjectReturnType.ColumnsFieldType.SpecFieldType spec);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface CountFieldType {
            @JsOverlay
            static Aggregation.ToObjectReturnType.CountFieldType create() {
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
            static Aggregation.ToObjectReturnType.FirstRowKeyFieldType create() {
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
            static Aggregation.ToObjectReturnType.PartitionFieldType create() {
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
        static Aggregation.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Aggregation.ToObjectReturnType.ColumnsFieldType getColumns();

        @JsProperty
        Aggregation.ToObjectReturnType.CountFieldType getCount();

        @JsProperty
        Aggregation.ToObjectReturnType.FirstRowKeyFieldType getFirstRowKey();

        @JsProperty
        Object getLastRowKey();

        @JsProperty
        Aggregation.ToObjectReturnType.PartitionFieldType getPartition();

        @JsProperty
        void setColumns(Aggregation.ToObjectReturnType.ColumnsFieldType columns);

        @JsProperty
        void setCount(Aggregation.ToObjectReturnType.CountFieldType count);

        @JsProperty
        void setFirstRowKey(Aggregation.ToObjectReturnType.FirstRowKeyFieldType firstRowKey);

        @JsProperty
        void setLastRowKey(Object lastRowKey);

        @JsProperty
        void setPartition(Aggregation.ToObjectReturnType.PartitionFieldType partition);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ColumnsFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface SpecFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface ApproximatePercentileFieldType {
                    @JsOverlay
                    static Aggregation.ToObjectReturnType0.ColumnsFieldType.SpecFieldType.ApproximatePercentileFieldType create() {
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
                    static Aggregation.ToObjectReturnType0.ColumnsFieldType.SpecFieldType.CountDistinctFieldType create() {
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
                    static Aggregation.ToObjectReturnType0.ColumnsFieldType.SpecFieldType.DistinctFieldType create() {
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
                    static Aggregation.ToObjectReturnType0.ColumnsFieldType.SpecFieldType.FormulaFieldType create() {
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
                    static Aggregation.ToObjectReturnType0.ColumnsFieldType.SpecFieldType.MedianFieldType create() {
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
                    static Aggregation.ToObjectReturnType0.ColumnsFieldType.SpecFieldType.PercentileFieldType create() {
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
                        static Aggregation.ToObjectReturnType0.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        String getColumnName();

                        @JsProperty
                        void setColumnName(String columnName);
                    }

                    @JsOverlay
                    static Aggregation.ToObjectReturnType0.ColumnsFieldType.SpecFieldType.SortedFirstFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    JsArray<Aggregation.ToObjectReturnType0.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType> getColumnsList();

                    @JsOverlay
                    default void setColumnsList(
                            Aggregation.ToObjectReturnType0.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType[] columnsList) {
                        setColumnsList(
                                Js.<JsArray<Aggregation.ToObjectReturnType0.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType>>uncheckedCast(
                                        columnsList));
                    }

                    @JsProperty
                    void setColumnsList(
                            JsArray<Aggregation.ToObjectReturnType0.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType> columnsList);
                }

                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface TDigestFieldType {
                    @JsOverlay
                    static Aggregation.ToObjectReturnType0.ColumnsFieldType.SpecFieldType.TDigestFieldType create() {
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
                        static Aggregation.ToObjectReturnType0.ColumnsFieldType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType create() {
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
                    static Aggregation.ToObjectReturnType0.ColumnsFieldType.SpecFieldType.UniqueFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    Aggregation.ToObjectReturnType0.ColumnsFieldType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType getNonUniqueSentinel();

                    @JsProperty
                    boolean isIncludeNulls();

                    @JsProperty
                    void setIncludeNulls(boolean includeNulls);

                    @JsProperty
                    void setNonUniqueSentinel(
                            Aggregation.ToObjectReturnType0.ColumnsFieldType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType nonUniqueSentinel);
                }

                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface WeightedAvgFieldType {
                    @JsOverlay
                    static Aggregation.ToObjectReturnType0.ColumnsFieldType.SpecFieldType.WeightedAvgFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    String getWeightColumn();

                    @JsProperty
                    void setWeightColumn(String weightColumn);
                }

                @JsOverlay
                static Aggregation.ToObjectReturnType0.ColumnsFieldType.SpecFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getAbsSum();

                @JsProperty
                Aggregation.ToObjectReturnType0.ColumnsFieldType.SpecFieldType.ApproximatePercentileFieldType getApproximatePercentile();

                @JsProperty
                Object getAvg();

                @JsProperty
                Aggregation.ToObjectReturnType0.ColumnsFieldType.SpecFieldType.CountDistinctFieldType getCountDistinct();

                @JsProperty
                Aggregation.ToObjectReturnType0.ColumnsFieldType.SpecFieldType.DistinctFieldType getDistinct();

                @JsProperty
                Object getFirst();

                @JsProperty
                Aggregation.ToObjectReturnType0.ColumnsFieldType.SpecFieldType.FormulaFieldType getFormula();

                @JsProperty
                Object getFreeze();

                @JsProperty
                Object getGroup();

                @JsProperty
                Object getLast();

                @JsProperty
                Object getMax();

                @JsProperty
                Aggregation.ToObjectReturnType0.ColumnsFieldType.SpecFieldType.MedianFieldType getMedian();

                @JsProperty
                Object getMin();

                @JsProperty
                Object getPb_var();

                @JsProperty
                Aggregation.ToObjectReturnType0.ColumnsFieldType.SpecFieldType.PercentileFieldType getPercentile();

                @JsProperty
                Aggregation.ToObjectReturnType0.ColumnsFieldType.SpecFieldType.SortedFirstFieldType getSortedFirst();

                @JsProperty
                Object getSortedLast();

                @JsProperty
                Object getStd();

                @JsProperty
                Object getSum();

                @JsProperty
                Aggregation.ToObjectReturnType0.ColumnsFieldType.SpecFieldType.TDigestFieldType getTDigest();

                @JsProperty
                Aggregation.ToObjectReturnType0.ColumnsFieldType.SpecFieldType.UniqueFieldType getUnique();

                @JsProperty
                Aggregation.ToObjectReturnType0.ColumnsFieldType.SpecFieldType.WeightedAvgFieldType getWeightedAvg();

                @JsProperty
                Object getWeightedSum();

                @JsProperty
                void setAbsSum(Object absSum);

                @JsProperty
                void setApproximatePercentile(
                        Aggregation.ToObjectReturnType0.ColumnsFieldType.SpecFieldType.ApproximatePercentileFieldType approximatePercentile);

                @JsProperty
                void setAvg(Object avg);

                @JsProperty
                void setCountDistinct(
                        Aggregation.ToObjectReturnType0.ColumnsFieldType.SpecFieldType.CountDistinctFieldType countDistinct);

                @JsProperty
                void setDistinct(
                        Aggregation.ToObjectReturnType0.ColumnsFieldType.SpecFieldType.DistinctFieldType distinct);

                @JsProperty
                void setFirst(Object first);

                @JsProperty
                void setFormula(
                        Aggregation.ToObjectReturnType0.ColumnsFieldType.SpecFieldType.FormulaFieldType formula);

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
                        Aggregation.ToObjectReturnType0.ColumnsFieldType.SpecFieldType.MedianFieldType median);

                @JsProperty
                void setMin(Object min);

                @JsProperty
                void setPb_var(Object pb_var);

                @JsProperty
                void setPercentile(
                        Aggregation.ToObjectReturnType0.ColumnsFieldType.SpecFieldType.PercentileFieldType percentile);

                @JsProperty
                void setSortedFirst(
                        Aggregation.ToObjectReturnType0.ColumnsFieldType.SpecFieldType.SortedFirstFieldType sortedFirst);

                @JsProperty
                void setSortedLast(Object sortedLast);

                @JsProperty
                void setStd(Object std);

                @JsProperty
                void setSum(Object sum);

                @JsProperty
                void setTDigest(
                        Aggregation.ToObjectReturnType0.ColumnsFieldType.SpecFieldType.TDigestFieldType tDigest);

                @JsProperty
                void setUnique(
                        Aggregation.ToObjectReturnType0.ColumnsFieldType.SpecFieldType.UniqueFieldType unique);

                @JsProperty
                void setWeightedAvg(
                        Aggregation.ToObjectReturnType0.ColumnsFieldType.SpecFieldType.WeightedAvgFieldType weightedAvg);

                @JsProperty
                void setWeightedSum(Object weightedSum);
            }

            @JsOverlay
            static Aggregation.ToObjectReturnType0.ColumnsFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<String> getMatchPairsList();

            @JsProperty
            Aggregation.ToObjectReturnType0.ColumnsFieldType.SpecFieldType getSpec();

            @JsProperty
            void setMatchPairsList(JsArray<String> matchPairsList);

            @JsOverlay
            default void setMatchPairsList(String[] matchPairsList) {
                setMatchPairsList(Js.<JsArray<String>>uncheckedCast(matchPairsList));
            }

            @JsProperty
            void setSpec(Aggregation.ToObjectReturnType0.ColumnsFieldType.SpecFieldType spec);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface CountFieldType {
            @JsOverlay
            static Aggregation.ToObjectReturnType0.CountFieldType create() {
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
            static Aggregation.ToObjectReturnType0.FirstRowKeyFieldType create() {
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
            static Aggregation.ToObjectReturnType0.PartitionFieldType create() {
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
        static Aggregation.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Aggregation.ToObjectReturnType0.ColumnsFieldType getColumns();

        @JsProperty
        Aggregation.ToObjectReturnType0.CountFieldType getCount();

        @JsProperty
        Aggregation.ToObjectReturnType0.FirstRowKeyFieldType getFirstRowKey();

        @JsProperty
        Object getLastRowKey();

        @JsProperty
        Aggregation.ToObjectReturnType0.PartitionFieldType getPartition();

        @JsProperty
        void setColumns(Aggregation.ToObjectReturnType0.ColumnsFieldType columns);

        @JsProperty
        void setCount(Aggregation.ToObjectReturnType0.CountFieldType count);

        @JsProperty
        void setFirstRowKey(Aggregation.ToObjectReturnType0.FirstRowKeyFieldType firstRowKey);

        @JsProperty
        void setLastRowKey(Object lastRowKey);

        @JsProperty
        void setPartition(Aggregation.ToObjectReturnType0.PartitionFieldType partition);
    }

    public static native Aggregation deserializeBinary(Uint8Array bytes);

    public static native Aggregation deserializeBinaryFromReader(Aggregation message, Object reader);

    public static native void serializeBinaryToWriter(Aggregation message, Object writer);

    public static native Aggregation.ToObjectReturnType toObject(
            boolean includeInstance, Aggregation msg);

    public native void clearColumns();

    public native void clearCount();

    public native void clearFirstRowKey();

    public native void clearLastRowKey();

    public native void clearPartition();

    public native AggregationColumns getColumns();

    public native AggregationCount getCount();

    public native AggregationRowKey getFirstRowKey();

    public native AggregationRowKey getLastRowKey();

    public native AggregationPartition getPartition();

    public native int getTypeCase();

    public native boolean hasColumns();

    public native boolean hasCount();

    public native boolean hasFirstRowKey();

    public native boolean hasLastRowKey();

    public native boolean hasPartition();

    public native Uint8Array serializeBinary();

    public native void setColumns();

    public native void setColumns(AggregationColumns value);

    public native void setCount();

    public native void setCount(AggregationCount value);

    public native void setFirstRowKey();

    public native void setFirstRowKey(AggregationRowKey value);

    public native void setLastRowKey();

    public native void setLastRowKey(AggregationRowKey value);

    public native void setPartition();

    public native void setPartition(AggregationPartition value);

    public native Aggregation.ToObjectReturnType0 toObject();

    public native Aggregation.ToObjectReturnType0 toObject(boolean includeInstance);
}
