//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.aggspec.AggSpecAbsSum;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.aggspec.AggSpecApproximatePercentile;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.aggspec.AggSpecAvg;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.aggspec.AggSpecCountDistinct;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.aggspec.AggSpecDistinct;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.aggspec.AggSpecFirst;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.aggspec.AggSpecFormula;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.aggspec.AggSpecFreeze;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.aggspec.AggSpecGroup;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.aggspec.AggSpecLast;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.aggspec.AggSpecMax;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.aggspec.AggSpecMedian;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.aggspec.AggSpecMin;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.aggspec.AggSpecPercentile;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.aggspec.AggSpecSorted;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.aggspec.AggSpecStd;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.aggspec.AggSpecSum;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.aggspec.AggSpecTDigest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.aggspec.AggSpecUnique;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.aggspec.AggSpecVar;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.aggspec.AggSpecWeighted;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.table_pb.AggSpec",
        namespace = JsPackage.GLOBAL)
public class AggSpec {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ApproximatePercentileFieldType {
            @JsOverlay
            static AggSpec.ToObjectReturnType.ApproximatePercentileFieldType create() {
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
            static AggSpec.ToObjectReturnType.CountDistinctFieldType create() {
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
            static AggSpec.ToObjectReturnType.DistinctFieldType create() {
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
            static AggSpec.ToObjectReturnType.FormulaFieldType create() {
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
            static AggSpec.ToObjectReturnType.MedianFieldType create() {
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
            static AggSpec.ToObjectReturnType.PercentileFieldType create() {
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
                static AggSpec.ToObjectReturnType.SortedFirstFieldType.ColumnsListFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getColumnName();

                @JsProperty
                void setColumnName(String columnName);
            }

            @JsOverlay
            static AggSpec.ToObjectReturnType.SortedFirstFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<AggSpec.ToObjectReturnType.SortedFirstFieldType.ColumnsListFieldType> getColumnsList();

            @JsOverlay
            default void setColumnsList(
                    AggSpec.ToObjectReturnType.SortedFirstFieldType.ColumnsListFieldType[] columnsList) {
                setColumnsList(
                        Js.<JsArray<AggSpec.ToObjectReturnType.SortedFirstFieldType.ColumnsListFieldType>>uncheckedCast(
                                columnsList));
            }

            @JsProperty
            void setColumnsList(
                    JsArray<AggSpec.ToObjectReturnType.SortedFirstFieldType.ColumnsListFieldType> columnsList);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface TDigestFieldType {
            @JsOverlay
            static AggSpec.ToObjectReturnType.TDigestFieldType create() {
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
                static AggSpec.ToObjectReturnType.UniqueFieldType.NonUniqueSentinelFieldType create() {
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
            static AggSpec.ToObjectReturnType.UniqueFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            AggSpec.ToObjectReturnType.UniqueFieldType.NonUniqueSentinelFieldType getNonUniqueSentinel();

            @JsProperty
            boolean isIncludeNulls();

            @JsProperty
            void setIncludeNulls(boolean includeNulls);

            @JsProperty
            void setNonUniqueSentinel(
                    AggSpec.ToObjectReturnType.UniqueFieldType.NonUniqueSentinelFieldType nonUniqueSentinel);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface WeightedAvgFieldType {
            @JsOverlay
            static AggSpec.ToObjectReturnType.WeightedAvgFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getWeightColumn();

            @JsProperty
            void setWeightColumn(String weightColumn);
        }

        @JsOverlay
        static AggSpec.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Object getAbsSum();

        @JsProperty
        AggSpec.ToObjectReturnType.ApproximatePercentileFieldType getApproximatePercentile();

        @JsProperty
        Object getAvg();

        @JsProperty
        AggSpec.ToObjectReturnType.CountDistinctFieldType getCountDistinct();

        @JsProperty
        AggSpec.ToObjectReturnType.DistinctFieldType getDistinct();

        @JsProperty
        Object getFirst();

        @JsProperty
        AggSpec.ToObjectReturnType.FormulaFieldType getFormula();

        @JsProperty
        Object getFreeze();

        @JsProperty
        Object getGroup();

        @JsProperty
        Object getLast();

        @JsProperty
        Object getMax();

        @JsProperty
        AggSpec.ToObjectReturnType.MedianFieldType getMedian();

        @JsProperty
        Object getMin();

        @JsProperty
        Object getPb_var();

        @JsProperty
        AggSpec.ToObjectReturnType.PercentileFieldType getPercentile();

        @JsProperty
        AggSpec.ToObjectReturnType.SortedFirstFieldType getSortedFirst();

        @JsProperty
        Object getSortedLast();

        @JsProperty
        Object getStd();

        @JsProperty
        Object getSum();

        @JsProperty
        AggSpec.ToObjectReturnType.TDigestFieldType getTDigest();

        @JsProperty
        AggSpec.ToObjectReturnType.UniqueFieldType getUnique();

        @JsProperty
        AggSpec.ToObjectReturnType.WeightedAvgFieldType getWeightedAvg();

        @JsProperty
        Object getWeightedSum();

        @JsProperty
        void setAbsSum(Object absSum);

        @JsProperty
        void setApproximatePercentile(
                AggSpec.ToObjectReturnType.ApproximatePercentileFieldType approximatePercentile);

        @JsProperty
        void setAvg(Object avg);

        @JsProperty
        void setCountDistinct(AggSpec.ToObjectReturnType.CountDistinctFieldType countDistinct);

        @JsProperty
        void setDistinct(AggSpec.ToObjectReturnType.DistinctFieldType distinct);

        @JsProperty
        void setFirst(Object first);

        @JsProperty
        void setFormula(AggSpec.ToObjectReturnType.FormulaFieldType formula);

        @JsProperty
        void setFreeze(Object freeze);

        @JsProperty
        void setGroup(Object group);

        @JsProperty
        void setLast(Object last);

        @JsProperty
        void setMax(Object max);

        @JsProperty
        void setMedian(AggSpec.ToObjectReturnType.MedianFieldType median);

        @JsProperty
        void setMin(Object min);

        @JsProperty
        void setPb_var(Object pb_var);

        @JsProperty
        void setPercentile(AggSpec.ToObjectReturnType.PercentileFieldType percentile);

        @JsProperty
        void setSortedFirst(AggSpec.ToObjectReturnType.SortedFirstFieldType sortedFirst);

        @JsProperty
        void setSortedLast(Object sortedLast);

        @JsProperty
        void setStd(Object std);

        @JsProperty
        void setSum(Object sum);

        @JsProperty
        void setTDigest(AggSpec.ToObjectReturnType.TDigestFieldType tDigest);

        @JsProperty
        void setUnique(AggSpec.ToObjectReturnType.UniqueFieldType unique);

        @JsProperty
        void setWeightedAvg(AggSpec.ToObjectReturnType.WeightedAvgFieldType weightedAvg);

        @JsProperty
        void setWeightedSum(Object weightedSum);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ApproximatePercentileFieldType {
            @JsOverlay
            static AggSpec.ToObjectReturnType0.ApproximatePercentileFieldType create() {
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
            static AggSpec.ToObjectReturnType0.CountDistinctFieldType create() {
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
            static AggSpec.ToObjectReturnType0.DistinctFieldType create() {
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
            static AggSpec.ToObjectReturnType0.FormulaFieldType create() {
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
            static AggSpec.ToObjectReturnType0.MedianFieldType create() {
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
            static AggSpec.ToObjectReturnType0.PercentileFieldType create() {
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
                static AggSpec.ToObjectReturnType0.SortedFirstFieldType.ColumnsListFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getColumnName();

                @JsProperty
                void setColumnName(String columnName);
            }

            @JsOverlay
            static AggSpec.ToObjectReturnType0.SortedFirstFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<AggSpec.ToObjectReturnType0.SortedFirstFieldType.ColumnsListFieldType> getColumnsList();

            @JsOverlay
            default void setColumnsList(
                    AggSpec.ToObjectReturnType0.SortedFirstFieldType.ColumnsListFieldType[] columnsList) {
                setColumnsList(
                        Js.<JsArray<AggSpec.ToObjectReturnType0.SortedFirstFieldType.ColumnsListFieldType>>uncheckedCast(
                                columnsList));
            }

            @JsProperty
            void setColumnsList(
                    JsArray<AggSpec.ToObjectReturnType0.SortedFirstFieldType.ColumnsListFieldType> columnsList);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface TDigestFieldType {
            @JsOverlay
            static AggSpec.ToObjectReturnType0.TDigestFieldType create() {
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
                static AggSpec.ToObjectReturnType0.UniqueFieldType.NonUniqueSentinelFieldType create() {
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
            static AggSpec.ToObjectReturnType0.UniqueFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            AggSpec.ToObjectReturnType0.UniqueFieldType.NonUniqueSentinelFieldType getNonUniqueSentinel();

            @JsProperty
            boolean isIncludeNulls();

            @JsProperty
            void setIncludeNulls(boolean includeNulls);

            @JsProperty
            void setNonUniqueSentinel(
                    AggSpec.ToObjectReturnType0.UniqueFieldType.NonUniqueSentinelFieldType nonUniqueSentinel);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface WeightedAvgFieldType {
            @JsOverlay
            static AggSpec.ToObjectReturnType0.WeightedAvgFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getWeightColumn();

            @JsProperty
            void setWeightColumn(String weightColumn);
        }

        @JsOverlay
        static AggSpec.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Object getAbsSum();

        @JsProperty
        AggSpec.ToObjectReturnType0.ApproximatePercentileFieldType getApproximatePercentile();

        @JsProperty
        Object getAvg();

        @JsProperty
        AggSpec.ToObjectReturnType0.CountDistinctFieldType getCountDistinct();

        @JsProperty
        AggSpec.ToObjectReturnType0.DistinctFieldType getDistinct();

        @JsProperty
        Object getFirst();

        @JsProperty
        AggSpec.ToObjectReturnType0.FormulaFieldType getFormula();

        @JsProperty
        Object getFreeze();

        @JsProperty
        Object getGroup();

        @JsProperty
        Object getLast();

        @JsProperty
        Object getMax();

        @JsProperty
        AggSpec.ToObjectReturnType0.MedianFieldType getMedian();

        @JsProperty
        Object getMin();

        @JsProperty
        Object getPb_var();

        @JsProperty
        AggSpec.ToObjectReturnType0.PercentileFieldType getPercentile();

        @JsProperty
        AggSpec.ToObjectReturnType0.SortedFirstFieldType getSortedFirst();

        @JsProperty
        Object getSortedLast();

        @JsProperty
        Object getStd();

        @JsProperty
        Object getSum();

        @JsProperty
        AggSpec.ToObjectReturnType0.TDigestFieldType getTDigest();

        @JsProperty
        AggSpec.ToObjectReturnType0.UniqueFieldType getUnique();

        @JsProperty
        AggSpec.ToObjectReturnType0.WeightedAvgFieldType getWeightedAvg();

        @JsProperty
        Object getWeightedSum();

        @JsProperty
        void setAbsSum(Object absSum);

        @JsProperty
        void setApproximatePercentile(
                AggSpec.ToObjectReturnType0.ApproximatePercentileFieldType approximatePercentile);

        @JsProperty
        void setAvg(Object avg);

        @JsProperty
        void setCountDistinct(AggSpec.ToObjectReturnType0.CountDistinctFieldType countDistinct);

        @JsProperty
        void setDistinct(AggSpec.ToObjectReturnType0.DistinctFieldType distinct);

        @JsProperty
        void setFirst(Object first);

        @JsProperty
        void setFormula(AggSpec.ToObjectReturnType0.FormulaFieldType formula);

        @JsProperty
        void setFreeze(Object freeze);

        @JsProperty
        void setGroup(Object group);

        @JsProperty
        void setLast(Object last);

        @JsProperty
        void setMax(Object max);

        @JsProperty
        void setMedian(AggSpec.ToObjectReturnType0.MedianFieldType median);

        @JsProperty
        void setMin(Object min);

        @JsProperty
        void setPb_var(Object pb_var);

        @JsProperty
        void setPercentile(AggSpec.ToObjectReturnType0.PercentileFieldType percentile);

        @JsProperty
        void setSortedFirst(AggSpec.ToObjectReturnType0.SortedFirstFieldType sortedFirst);

        @JsProperty
        void setSortedLast(Object sortedLast);

        @JsProperty
        void setStd(Object std);

        @JsProperty
        void setSum(Object sum);

        @JsProperty
        void setTDigest(AggSpec.ToObjectReturnType0.TDigestFieldType tDigest);

        @JsProperty
        void setUnique(AggSpec.ToObjectReturnType0.UniqueFieldType unique);

        @JsProperty
        void setWeightedAvg(AggSpec.ToObjectReturnType0.WeightedAvgFieldType weightedAvg);

        @JsProperty
        void setWeightedSum(Object weightedSum);
    }

    public static native AggSpec deserializeBinary(Uint8Array bytes);

    public static native AggSpec deserializeBinaryFromReader(AggSpec message, Object reader);

    public static native void serializeBinaryToWriter(AggSpec message, Object writer);

    public static native AggSpec.ToObjectReturnType toObject(boolean includeInstance, AggSpec msg);

    public native void clearAbsSum();

    public native void clearApproximatePercentile();

    public native void clearAvg();

    public native void clearCountDistinct();

    public native void clearDistinct();

    public native void clearFirst();

    public native void clearFormula();

    public native void clearFreeze();

    public native void clearGroup();

    public native void clearLast();

    public native void clearMax();

    public native void clearMedian();

    public native void clearMin();

    public native void clearPercentile();

    public native void clearSortedFirst();

    public native void clearSortedLast();

    public native void clearStd();

    public native void clearSum();

    public native void clearTDigest();

    public native void clearUnique();

    public native void clearVar();

    public native void clearWeightedAvg();

    public native void clearWeightedSum();

    public native AggSpecAbsSum getAbsSum();

    public native AggSpecApproximatePercentile getApproximatePercentile();

    public native AggSpecAvg getAvg();

    public native AggSpecCountDistinct getCountDistinct();

    public native AggSpecDistinct getDistinct();

    public native AggSpecFirst getFirst();

    public native AggSpecFormula getFormula();

    public native AggSpecFreeze getFreeze();

    public native AggSpecGroup getGroup();

    public native AggSpecLast getLast();

    public native AggSpecMax getMax();

    public native AggSpecMedian getMedian();

    public native AggSpecMin getMin();

    public native AggSpecPercentile getPercentile();

    public native AggSpecSorted getSortedFirst();

    public native AggSpecSorted getSortedLast();

    public native AggSpecStd getStd();

    public native AggSpecSum getSum();

    public native AggSpecTDigest getTDigest();

    public native int getTypeCase();

    public native AggSpecUnique getUnique();

    public native AggSpecVar getVar();

    public native AggSpecWeighted getWeightedAvg();

    public native AggSpecWeighted getWeightedSum();

    public native boolean hasAbsSum();

    public native boolean hasApproximatePercentile();

    public native boolean hasAvg();

    public native boolean hasCountDistinct();

    public native boolean hasDistinct();

    public native boolean hasFirst();

    public native boolean hasFormula();

    public native boolean hasFreeze();

    public native boolean hasGroup();

    public native boolean hasLast();

    public native boolean hasMax();

    public native boolean hasMedian();

    public native boolean hasMin();

    public native boolean hasPercentile();

    public native boolean hasSortedFirst();

    public native boolean hasSortedLast();

    public native boolean hasStd();

    public native boolean hasSum();

    public native boolean hasTDigest();

    public native boolean hasUnique();

    public native boolean hasVar();

    public native boolean hasWeightedAvg();

    public native boolean hasWeightedSum();

    public native Uint8Array serializeBinary();

    public native void setAbsSum();

    public native void setAbsSum(AggSpecAbsSum value);

    public native void setApproximatePercentile();

    public native void setApproximatePercentile(AggSpecApproximatePercentile value);

    public native void setAvg();

    public native void setAvg(AggSpecAvg value);

    public native void setCountDistinct();

    public native void setCountDistinct(AggSpecCountDistinct value);

    public native void setDistinct();

    public native void setDistinct(AggSpecDistinct value);

    public native void setFirst();

    public native void setFirst(AggSpecFirst value);

    public native void setFormula();

    public native void setFormula(AggSpecFormula value);

    public native void setFreeze();

    public native void setFreeze(AggSpecFreeze value);

    public native void setGroup();

    public native void setGroup(AggSpecGroup value);

    public native void setLast();

    public native void setLast(AggSpecLast value);

    public native void setMax();

    public native void setMax(AggSpecMax value);

    public native void setMedian();

    public native void setMedian(AggSpecMedian value);

    public native void setMin();

    public native void setMin(AggSpecMin value);

    public native void setPercentile();

    public native void setPercentile(AggSpecPercentile value);

    public native void setSortedFirst();

    public native void setSortedFirst(AggSpecSorted value);

    public native void setSortedLast();

    public native void setSortedLast(AggSpecSorted value);

    public native void setStd();

    public native void setStd(AggSpecStd value);

    public native void setSum();

    public native void setSum(AggSpecSum value);

    public native void setTDigest();

    public native void setTDigest(AggSpecTDigest value);

    public native void setUnique();

    public native void setUnique(AggSpecUnique value);

    public native void setVar();

    public native void setVar(AggSpecVar value);

    public native void setWeightedAvg();

    public native void setWeightedAvg(AggSpecWeighted value);

    public native void setWeightedSum();

    public native void setWeightedSum(AggSpecWeighted value);

    public native AggSpec.ToObjectReturnType0 toObject();

    public native AggSpec.ToObjectReturnType0 toObject(boolean includeInstance);
}
