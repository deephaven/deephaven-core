//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.aggregation;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.AggSpec;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.table_pb.Aggregation.AggregationColumns",
        namespace = JsPackage.GLOBAL)
public class AggregationColumns {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SpecFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ApproximatePercentileFieldType {
                @JsOverlay
                static AggregationColumns.ToObjectReturnType.SpecFieldType.ApproximatePercentileFieldType create() {
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
                static AggregationColumns.ToObjectReturnType.SpecFieldType.CountDistinctFieldType create() {
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
                static AggregationColumns.ToObjectReturnType.SpecFieldType.DistinctFieldType create() {
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
                static AggregationColumns.ToObjectReturnType.SpecFieldType.FormulaFieldType create() {
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
                static AggregationColumns.ToObjectReturnType.SpecFieldType.MedianFieldType create() {
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
                static AggregationColumns.ToObjectReturnType.SpecFieldType.PercentileFieldType create() {
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
                    static AggregationColumns.ToObjectReturnType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    String getColumnName();

                    @JsProperty
                    void setColumnName(String columnName);
                }

                @JsOverlay
                static AggregationColumns.ToObjectReturnType.SpecFieldType.SortedFirstFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<AggregationColumns.ToObjectReturnType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType> getColumnsList();

                @JsOverlay
                default void setColumnsList(
                        AggregationColumns.ToObjectReturnType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType[] columnsList) {
                    setColumnsList(
                            Js.<JsArray<AggregationColumns.ToObjectReturnType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType>>uncheckedCast(
                                    columnsList));
                }

                @JsProperty
                void setColumnsList(
                        JsArray<AggregationColumns.ToObjectReturnType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType> columnsList);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TDigestFieldType {
                @JsOverlay
                static AggregationColumns.ToObjectReturnType.SpecFieldType.TDigestFieldType create() {
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
                    static AggregationColumns.ToObjectReturnType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType create() {
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
                static AggregationColumns.ToObjectReturnType.SpecFieldType.UniqueFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                AggregationColumns.ToObjectReturnType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType getNonUniqueSentinel();

                @JsProperty
                boolean isIncludeNulls();

                @JsProperty
                void setIncludeNulls(boolean includeNulls);

                @JsProperty
                void setNonUniqueSentinel(
                        AggregationColumns.ToObjectReturnType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType nonUniqueSentinel);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface WeightedAvgFieldType {
                @JsOverlay
                static AggregationColumns.ToObjectReturnType.SpecFieldType.WeightedAvgFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getWeightColumn();

                @JsProperty
                void setWeightColumn(String weightColumn);
            }

            @JsOverlay
            static AggregationColumns.ToObjectReturnType.SpecFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            Object getAbsSum();

            @JsProperty
            AggregationColumns.ToObjectReturnType.SpecFieldType.ApproximatePercentileFieldType getApproximatePercentile();

            @JsProperty
            Object getAvg();

            @JsProperty
            AggregationColumns.ToObjectReturnType.SpecFieldType.CountDistinctFieldType getCountDistinct();

            @JsProperty
            AggregationColumns.ToObjectReturnType.SpecFieldType.DistinctFieldType getDistinct();

            @JsProperty
            Object getFirst();

            @JsProperty
            AggregationColumns.ToObjectReturnType.SpecFieldType.FormulaFieldType getFormula();

            @JsProperty
            Object getFreeze();

            @JsProperty
            Object getGroup();

            @JsProperty
            Object getLast();

            @JsProperty
            Object getMax();

            @JsProperty
            AggregationColumns.ToObjectReturnType.SpecFieldType.MedianFieldType getMedian();

            @JsProperty
            Object getMin();

            @JsProperty
            Object getPb_var();

            @JsProperty
            AggregationColumns.ToObjectReturnType.SpecFieldType.PercentileFieldType getPercentile();

            @JsProperty
            AggregationColumns.ToObjectReturnType.SpecFieldType.SortedFirstFieldType getSortedFirst();

            @JsProperty
            Object getSortedLast();

            @JsProperty
            Object getStd();

            @JsProperty
            Object getSum();

            @JsProperty
            AggregationColumns.ToObjectReturnType.SpecFieldType.TDigestFieldType getTDigest();

            @JsProperty
            AggregationColumns.ToObjectReturnType.SpecFieldType.UniqueFieldType getUnique();

            @JsProperty
            AggregationColumns.ToObjectReturnType.SpecFieldType.WeightedAvgFieldType getWeightedAvg();

            @JsProperty
            Object getWeightedSum();

            @JsProperty
            void setAbsSum(Object absSum);

            @JsProperty
            void setApproximatePercentile(
                    AggregationColumns.ToObjectReturnType.SpecFieldType.ApproximatePercentileFieldType approximatePercentile);

            @JsProperty
            void setAvg(Object avg);

            @JsProperty
            void setCountDistinct(
                    AggregationColumns.ToObjectReturnType.SpecFieldType.CountDistinctFieldType countDistinct);

            @JsProperty
            void setDistinct(
                    AggregationColumns.ToObjectReturnType.SpecFieldType.DistinctFieldType distinct);

            @JsProperty
            void setFirst(Object first);

            @JsProperty
            void setFormula(AggregationColumns.ToObjectReturnType.SpecFieldType.FormulaFieldType formula);

            @JsProperty
            void setFreeze(Object freeze);

            @JsProperty
            void setGroup(Object group);

            @JsProperty
            void setLast(Object last);

            @JsProperty
            void setMax(Object max);

            @JsProperty
            void setMedian(AggregationColumns.ToObjectReturnType.SpecFieldType.MedianFieldType median);

            @JsProperty
            void setMin(Object min);

            @JsProperty
            void setPb_var(Object pb_var);

            @JsProperty
            void setPercentile(
                    AggregationColumns.ToObjectReturnType.SpecFieldType.PercentileFieldType percentile);

            @JsProperty
            void setSortedFirst(
                    AggregationColumns.ToObjectReturnType.SpecFieldType.SortedFirstFieldType sortedFirst);

            @JsProperty
            void setSortedLast(Object sortedLast);

            @JsProperty
            void setStd(Object std);

            @JsProperty
            void setSum(Object sum);

            @JsProperty
            void setTDigest(AggregationColumns.ToObjectReturnType.SpecFieldType.TDigestFieldType tDigest);

            @JsProperty
            void setUnique(AggregationColumns.ToObjectReturnType.SpecFieldType.UniqueFieldType unique);

            @JsProperty
            void setWeightedAvg(
                    AggregationColumns.ToObjectReturnType.SpecFieldType.WeightedAvgFieldType weightedAvg);

            @JsProperty
            void setWeightedSum(Object weightedSum);
        }

        @JsOverlay
        static AggregationColumns.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<String> getMatchPairsList();

        @JsProperty
        AggregationColumns.ToObjectReturnType.SpecFieldType getSpec();

        @JsProperty
        void setMatchPairsList(JsArray<String> matchPairsList);

        @JsOverlay
        default void setMatchPairsList(String[] matchPairsList) {
            setMatchPairsList(Js.<JsArray<String>>uncheckedCast(matchPairsList));
        }

        @JsProperty
        void setSpec(AggregationColumns.ToObjectReturnType.SpecFieldType spec);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SpecFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ApproximatePercentileFieldType {
                @JsOverlay
                static AggregationColumns.ToObjectReturnType0.SpecFieldType.ApproximatePercentileFieldType create() {
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
                static AggregationColumns.ToObjectReturnType0.SpecFieldType.CountDistinctFieldType create() {
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
                static AggregationColumns.ToObjectReturnType0.SpecFieldType.DistinctFieldType create() {
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
                static AggregationColumns.ToObjectReturnType0.SpecFieldType.FormulaFieldType create() {
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
                static AggregationColumns.ToObjectReturnType0.SpecFieldType.MedianFieldType create() {
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
                static AggregationColumns.ToObjectReturnType0.SpecFieldType.PercentileFieldType create() {
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
                    static AggregationColumns.ToObjectReturnType0.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    String getColumnName();

                    @JsProperty
                    void setColumnName(String columnName);
                }

                @JsOverlay
                static AggregationColumns.ToObjectReturnType0.SpecFieldType.SortedFirstFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<AggregationColumns.ToObjectReturnType0.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType> getColumnsList();

                @JsOverlay
                default void setColumnsList(
                        AggregationColumns.ToObjectReturnType0.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType[] columnsList) {
                    setColumnsList(
                            Js.<JsArray<AggregationColumns.ToObjectReturnType0.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType>>uncheckedCast(
                                    columnsList));
                }

                @JsProperty
                void setColumnsList(
                        JsArray<AggregationColumns.ToObjectReturnType0.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType> columnsList);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TDigestFieldType {
                @JsOverlay
                static AggregationColumns.ToObjectReturnType0.SpecFieldType.TDigestFieldType create() {
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
                    static AggregationColumns.ToObjectReturnType0.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType create() {
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
                static AggregationColumns.ToObjectReturnType0.SpecFieldType.UniqueFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                AggregationColumns.ToObjectReturnType0.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType getNonUniqueSentinel();

                @JsProperty
                boolean isIncludeNulls();

                @JsProperty
                void setIncludeNulls(boolean includeNulls);

                @JsProperty
                void setNonUniqueSentinel(
                        AggregationColumns.ToObjectReturnType0.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType nonUniqueSentinel);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface WeightedAvgFieldType {
                @JsOverlay
                static AggregationColumns.ToObjectReturnType0.SpecFieldType.WeightedAvgFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getWeightColumn();

                @JsProperty
                void setWeightColumn(String weightColumn);
            }

            @JsOverlay
            static AggregationColumns.ToObjectReturnType0.SpecFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            Object getAbsSum();

            @JsProperty
            AggregationColumns.ToObjectReturnType0.SpecFieldType.ApproximatePercentileFieldType getApproximatePercentile();

            @JsProperty
            Object getAvg();

            @JsProperty
            AggregationColumns.ToObjectReturnType0.SpecFieldType.CountDistinctFieldType getCountDistinct();

            @JsProperty
            AggregationColumns.ToObjectReturnType0.SpecFieldType.DistinctFieldType getDistinct();

            @JsProperty
            Object getFirst();

            @JsProperty
            AggregationColumns.ToObjectReturnType0.SpecFieldType.FormulaFieldType getFormula();

            @JsProperty
            Object getFreeze();

            @JsProperty
            Object getGroup();

            @JsProperty
            Object getLast();

            @JsProperty
            Object getMax();

            @JsProperty
            AggregationColumns.ToObjectReturnType0.SpecFieldType.MedianFieldType getMedian();

            @JsProperty
            Object getMin();

            @JsProperty
            Object getPb_var();

            @JsProperty
            AggregationColumns.ToObjectReturnType0.SpecFieldType.PercentileFieldType getPercentile();

            @JsProperty
            AggregationColumns.ToObjectReturnType0.SpecFieldType.SortedFirstFieldType getSortedFirst();

            @JsProperty
            Object getSortedLast();

            @JsProperty
            Object getStd();

            @JsProperty
            Object getSum();

            @JsProperty
            AggregationColumns.ToObjectReturnType0.SpecFieldType.TDigestFieldType getTDigest();

            @JsProperty
            AggregationColumns.ToObjectReturnType0.SpecFieldType.UniqueFieldType getUnique();

            @JsProperty
            AggregationColumns.ToObjectReturnType0.SpecFieldType.WeightedAvgFieldType getWeightedAvg();

            @JsProperty
            Object getWeightedSum();

            @JsProperty
            void setAbsSum(Object absSum);

            @JsProperty
            void setApproximatePercentile(
                    AggregationColumns.ToObjectReturnType0.SpecFieldType.ApproximatePercentileFieldType approximatePercentile);

            @JsProperty
            void setAvg(Object avg);

            @JsProperty
            void setCountDistinct(
                    AggregationColumns.ToObjectReturnType0.SpecFieldType.CountDistinctFieldType countDistinct);

            @JsProperty
            void setDistinct(
                    AggregationColumns.ToObjectReturnType0.SpecFieldType.DistinctFieldType distinct);

            @JsProperty
            void setFirst(Object first);

            @JsProperty
            void setFormula(
                    AggregationColumns.ToObjectReturnType0.SpecFieldType.FormulaFieldType formula);

            @JsProperty
            void setFreeze(Object freeze);

            @JsProperty
            void setGroup(Object group);

            @JsProperty
            void setLast(Object last);

            @JsProperty
            void setMax(Object max);

            @JsProperty
            void setMedian(AggregationColumns.ToObjectReturnType0.SpecFieldType.MedianFieldType median);

            @JsProperty
            void setMin(Object min);

            @JsProperty
            void setPb_var(Object pb_var);

            @JsProperty
            void setPercentile(
                    AggregationColumns.ToObjectReturnType0.SpecFieldType.PercentileFieldType percentile);

            @JsProperty
            void setSortedFirst(
                    AggregationColumns.ToObjectReturnType0.SpecFieldType.SortedFirstFieldType sortedFirst);

            @JsProperty
            void setSortedLast(Object sortedLast);

            @JsProperty
            void setStd(Object std);

            @JsProperty
            void setSum(Object sum);

            @JsProperty
            void setTDigest(
                    AggregationColumns.ToObjectReturnType0.SpecFieldType.TDigestFieldType tDigest);

            @JsProperty
            void setUnique(AggregationColumns.ToObjectReturnType0.SpecFieldType.UniqueFieldType unique);

            @JsProperty
            void setWeightedAvg(
                    AggregationColumns.ToObjectReturnType0.SpecFieldType.WeightedAvgFieldType weightedAvg);

            @JsProperty
            void setWeightedSum(Object weightedSum);
        }

        @JsOverlay
        static AggregationColumns.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<String> getMatchPairsList();

        @JsProperty
        AggregationColumns.ToObjectReturnType0.SpecFieldType getSpec();

        @JsProperty
        void setMatchPairsList(JsArray<String> matchPairsList);

        @JsOverlay
        default void setMatchPairsList(String[] matchPairsList) {
            setMatchPairsList(Js.<JsArray<String>>uncheckedCast(matchPairsList));
        }

        @JsProperty
        void setSpec(AggregationColumns.ToObjectReturnType0.SpecFieldType spec);
    }

    public static native AggregationColumns deserializeBinary(Uint8Array bytes);

    public static native AggregationColumns deserializeBinaryFromReader(
            AggregationColumns message, Object reader);

    public static native void serializeBinaryToWriter(AggregationColumns message, Object writer);

    public static native AggregationColumns.ToObjectReturnType toObject(
            boolean includeInstance, AggregationColumns msg);

    public native String addMatchPairs(String value, double index);

    public native String addMatchPairs(String value);

    public native void clearMatchPairsList();

    public native void clearSpec();

    public native JsArray<String> getMatchPairsList();

    public native AggSpec getSpec();

    public native boolean hasSpec();

    public native Uint8Array serializeBinary();

    public native void setMatchPairsList(JsArray<String> value);

    @JsOverlay
    public final void setMatchPairsList(String[] value) {
        setMatchPairsList(Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void setSpec();

    public native void setSpec(AggSpec value);

    public native AggregationColumns.ToObjectReturnType0 toObject();

    public native AggregationColumns.ToObjectReturnType0 toObject(boolean includeInstance);
}
