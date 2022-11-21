package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.Ticket;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.AggAllByRequest",
        namespace = JsPackage.GLOBAL)
public class AggAllByRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static AggAllByRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType of(
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
            static AggAllByRequest.ToObjectReturnType.ResultIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            AggAllByRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    AggAllByRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<AggAllByRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<AggAllByRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SourceIdFieldType {
            @JsOverlay
            static AggAllByRequest.ToObjectReturnType.SourceIdFieldType create() {
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
                static AggAllByRequest.ToObjectReturnType.SpecFieldType.ApproximatePercentileFieldType create() {
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
                static AggAllByRequest.ToObjectReturnType.SpecFieldType.CountDistinctFieldType create() {
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
                static AggAllByRequest.ToObjectReturnType.SpecFieldType.DistinctFieldType create() {
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
                static AggAllByRequest.ToObjectReturnType.SpecFieldType.FormulaFieldType create() {
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
                static AggAllByRequest.ToObjectReturnType.SpecFieldType.MedianFieldType create() {
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
                static AggAllByRequest.ToObjectReturnType.SpecFieldType.PercentileFieldType create() {
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
                    static AggAllByRequest.ToObjectReturnType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType create() {
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
                static AggAllByRequest.ToObjectReturnType.SpecFieldType.SortedFirstFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<AggAllByRequest.ToObjectReturnType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType> getColumnsList();

                @JsOverlay
                default void setColumnsList(
                        AggAllByRequest.ToObjectReturnType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType[] columnsList) {
                    setColumnsList(
                            Js.<JsArray<AggAllByRequest.ToObjectReturnType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType>>uncheckedCast(
                                    columnsList));
                }

                @JsProperty
                void setColumnsList(
                        JsArray<AggAllByRequest.ToObjectReturnType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType> columnsList);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TDigestFieldType {
                @JsOverlay
                static AggAllByRequest.ToObjectReturnType.SpecFieldType.TDigestFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                double getCompression();

                @JsProperty
                void setCompression(double compression);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface UniqueFieldType {
                @JsOverlay
                static AggAllByRequest.ToObjectReturnType.SpecFieldType.UniqueFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                boolean isIncludeNulls();

                @JsProperty
                void setIncludeNulls(boolean includeNulls);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface WeightedAvgFieldType {
                @JsOverlay
                static AggAllByRequest.ToObjectReturnType.SpecFieldType.WeightedAvgFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getWeightColumn();

                @JsProperty
                void setWeightColumn(String weightColumn);
            }

            @JsOverlay
            static AggAllByRequest.ToObjectReturnType.SpecFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            Object getAbsSum();

            @JsProperty
            AggAllByRequest.ToObjectReturnType.SpecFieldType.ApproximatePercentileFieldType getApproximatePercentile();

            @JsProperty
            Object getAvg();

            @JsProperty
            AggAllByRequest.ToObjectReturnType.SpecFieldType.CountDistinctFieldType getCountDistinct();

            @JsProperty
            AggAllByRequest.ToObjectReturnType.SpecFieldType.DistinctFieldType getDistinct();

            @JsProperty
            Object getFirst();

            @JsProperty
            AggAllByRequest.ToObjectReturnType.SpecFieldType.FormulaFieldType getFormula();

            @JsProperty
            Object getFreeze();

            @JsProperty
            Object getGroup();

            @JsProperty
            Object getLast();

            @JsProperty
            Object getMax();

            @JsProperty
            AggAllByRequest.ToObjectReturnType.SpecFieldType.MedianFieldType getMedian();

            @JsProperty
            Object getMin();

            @JsProperty
            Object getPb_var();

            @JsProperty
            AggAllByRequest.ToObjectReturnType.SpecFieldType.PercentileFieldType getPercentile();

            @JsProperty
            AggAllByRequest.ToObjectReturnType.SpecFieldType.SortedFirstFieldType getSortedFirst();

            @JsProperty
            Object getSortedLast();

            @JsProperty
            Object getStd();

            @JsProperty
            Object getSum();

            @JsProperty
            AggAllByRequest.ToObjectReturnType.SpecFieldType.TDigestFieldType getTDigest();

            @JsProperty
            AggAllByRequest.ToObjectReturnType.SpecFieldType.UniqueFieldType getUnique();

            @JsProperty
            AggAllByRequest.ToObjectReturnType.SpecFieldType.WeightedAvgFieldType getWeightedAvg();

            @JsProperty
            Object getWeightedSum();

            @JsProperty
            void setAbsSum(Object absSum);

            @JsProperty
            void setApproximatePercentile(
                    AggAllByRequest.ToObjectReturnType.SpecFieldType.ApproximatePercentileFieldType approximatePercentile);

            @JsProperty
            void setAvg(Object avg);

            @JsProperty
            void setCountDistinct(
                    AggAllByRequest.ToObjectReturnType.SpecFieldType.CountDistinctFieldType countDistinct);

            @JsProperty
            void setDistinct(AggAllByRequest.ToObjectReturnType.SpecFieldType.DistinctFieldType distinct);

            @JsProperty
            void setFirst(Object first);

            @JsProperty
            void setFormula(AggAllByRequest.ToObjectReturnType.SpecFieldType.FormulaFieldType formula);

            @JsProperty
            void setFreeze(Object freeze);

            @JsProperty
            void setGroup(Object group);

            @JsProperty
            void setLast(Object last);

            @JsProperty
            void setMax(Object max);

            @JsProperty
            void setMedian(AggAllByRequest.ToObjectReturnType.SpecFieldType.MedianFieldType median);

            @JsProperty
            void setMin(Object min);

            @JsProperty
            void setPb_var(Object pb_var);

            @JsProperty
            void setPercentile(
                    AggAllByRequest.ToObjectReturnType.SpecFieldType.PercentileFieldType percentile);

            @JsProperty
            void setSortedFirst(
                    AggAllByRequest.ToObjectReturnType.SpecFieldType.SortedFirstFieldType sortedFirst);

            @JsProperty
            void setSortedLast(Object sortedLast);

            @JsProperty
            void setStd(Object std);

            @JsProperty
            void setSum(Object sum);

            @JsProperty
            void setTDigest(AggAllByRequest.ToObjectReturnType.SpecFieldType.TDigestFieldType tDigest);

            @JsProperty
            void setUnique(AggAllByRequest.ToObjectReturnType.SpecFieldType.UniqueFieldType unique);

            @JsProperty
            void setWeightedAvg(
                    AggAllByRequest.ToObjectReturnType.SpecFieldType.WeightedAvgFieldType weightedAvg);

            @JsProperty
            void setWeightedSum(Object weightedSum);
        }

        @JsOverlay
        static AggAllByRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<String> getGroupByColumnsList();

        @JsProperty
        AggAllByRequest.ToObjectReturnType.ResultIdFieldType getResultId();

        @JsProperty
        AggAllByRequest.ToObjectReturnType.SourceIdFieldType getSourceId();

        @JsProperty
        AggAllByRequest.ToObjectReturnType.SpecFieldType getSpec();

        @JsProperty
        void setGroupByColumnsList(JsArray<String> groupByColumnsList);

        @JsOverlay
        default void setGroupByColumnsList(String[] groupByColumnsList) {
            setGroupByColumnsList(Js.<JsArray<String>>uncheckedCast(groupByColumnsList));
        }

        @JsProperty
        void setResultId(AggAllByRequest.ToObjectReturnType.ResultIdFieldType resultId);

        @JsProperty
        void setSourceId(AggAllByRequest.ToObjectReturnType.SourceIdFieldType sourceId);

        @JsProperty
        void setSpec(AggAllByRequest.ToObjectReturnType.SpecFieldType spec);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static AggAllByRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType of(
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
            static AggAllByRequest.ToObjectReturnType0.ResultIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            AggAllByRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    AggAllByRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<AggAllByRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<AggAllByRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SourceIdFieldType {
            @JsOverlay
            static AggAllByRequest.ToObjectReturnType0.SourceIdFieldType create() {
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
                static AggAllByRequest.ToObjectReturnType0.SpecFieldType.ApproximatePercentileFieldType create() {
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
                static AggAllByRequest.ToObjectReturnType0.SpecFieldType.CountDistinctFieldType create() {
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
                static AggAllByRequest.ToObjectReturnType0.SpecFieldType.DistinctFieldType create() {
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
                static AggAllByRequest.ToObjectReturnType0.SpecFieldType.FormulaFieldType create() {
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
                static AggAllByRequest.ToObjectReturnType0.SpecFieldType.MedianFieldType create() {
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
                static AggAllByRequest.ToObjectReturnType0.SpecFieldType.PercentileFieldType create() {
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
                    static AggAllByRequest.ToObjectReturnType0.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType create() {
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
                static AggAllByRequest.ToObjectReturnType0.SpecFieldType.SortedFirstFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<AggAllByRequest.ToObjectReturnType0.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType> getColumnsList();

                @JsOverlay
                default void setColumnsList(
                        AggAllByRequest.ToObjectReturnType0.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType[] columnsList) {
                    setColumnsList(
                            Js.<JsArray<AggAllByRequest.ToObjectReturnType0.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType>>uncheckedCast(
                                    columnsList));
                }

                @JsProperty
                void setColumnsList(
                        JsArray<AggAllByRequest.ToObjectReturnType0.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType> columnsList);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TDigestFieldType {
                @JsOverlay
                static AggAllByRequest.ToObjectReturnType0.SpecFieldType.TDigestFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                double getCompression();

                @JsProperty
                void setCompression(double compression);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface UniqueFieldType {
                @JsOverlay
                static AggAllByRequest.ToObjectReturnType0.SpecFieldType.UniqueFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                boolean isIncludeNulls();

                @JsProperty
                void setIncludeNulls(boolean includeNulls);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface WeightedAvgFieldType {
                @JsOverlay
                static AggAllByRequest.ToObjectReturnType0.SpecFieldType.WeightedAvgFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getWeightColumn();

                @JsProperty
                void setWeightColumn(String weightColumn);
            }

            @JsOverlay
            static AggAllByRequest.ToObjectReturnType0.SpecFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            Object getAbsSum();

            @JsProperty
            AggAllByRequest.ToObjectReturnType0.SpecFieldType.ApproximatePercentileFieldType getApproximatePercentile();

            @JsProperty
            Object getAvg();

            @JsProperty
            AggAllByRequest.ToObjectReturnType0.SpecFieldType.CountDistinctFieldType getCountDistinct();

            @JsProperty
            AggAllByRequest.ToObjectReturnType0.SpecFieldType.DistinctFieldType getDistinct();

            @JsProperty
            Object getFirst();

            @JsProperty
            AggAllByRequest.ToObjectReturnType0.SpecFieldType.FormulaFieldType getFormula();

            @JsProperty
            Object getFreeze();

            @JsProperty
            Object getGroup();

            @JsProperty
            Object getLast();

            @JsProperty
            Object getMax();

            @JsProperty
            AggAllByRequest.ToObjectReturnType0.SpecFieldType.MedianFieldType getMedian();

            @JsProperty
            Object getMin();

            @JsProperty
            Object getPb_var();

            @JsProperty
            AggAllByRequest.ToObjectReturnType0.SpecFieldType.PercentileFieldType getPercentile();

            @JsProperty
            AggAllByRequest.ToObjectReturnType0.SpecFieldType.SortedFirstFieldType getSortedFirst();

            @JsProperty
            Object getSortedLast();

            @JsProperty
            Object getStd();

            @JsProperty
            Object getSum();

            @JsProperty
            AggAllByRequest.ToObjectReturnType0.SpecFieldType.TDigestFieldType getTDigest();

            @JsProperty
            AggAllByRequest.ToObjectReturnType0.SpecFieldType.UniqueFieldType getUnique();

            @JsProperty
            AggAllByRequest.ToObjectReturnType0.SpecFieldType.WeightedAvgFieldType getWeightedAvg();

            @JsProperty
            Object getWeightedSum();

            @JsProperty
            void setAbsSum(Object absSum);

            @JsProperty
            void setApproximatePercentile(
                    AggAllByRequest.ToObjectReturnType0.SpecFieldType.ApproximatePercentileFieldType approximatePercentile);

            @JsProperty
            void setAvg(Object avg);

            @JsProperty
            void setCountDistinct(
                    AggAllByRequest.ToObjectReturnType0.SpecFieldType.CountDistinctFieldType countDistinct);

            @JsProperty
            void setDistinct(
                    AggAllByRequest.ToObjectReturnType0.SpecFieldType.DistinctFieldType distinct);

            @JsProperty
            void setFirst(Object first);

            @JsProperty
            void setFormula(AggAllByRequest.ToObjectReturnType0.SpecFieldType.FormulaFieldType formula);

            @JsProperty
            void setFreeze(Object freeze);

            @JsProperty
            void setGroup(Object group);

            @JsProperty
            void setLast(Object last);

            @JsProperty
            void setMax(Object max);

            @JsProperty
            void setMedian(AggAllByRequest.ToObjectReturnType0.SpecFieldType.MedianFieldType median);

            @JsProperty
            void setMin(Object min);

            @JsProperty
            void setPb_var(Object pb_var);

            @JsProperty
            void setPercentile(
                    AggAllByRequest.ToObjectReturnType0.SpecFieldType.PercentileFieldType percentile);

            @JsProperty
            void setSortedFirst(
                    AggAllByRequest.ToObjectReturnType0.SpecFieldType.SortedFirstFieldType sortedFirst);

            @JsProperty
            void setSortedLast(Object sortedLast);

            @JsProperty
            void setStd(Object std);

            @JsProperty
            void setSum(Object sum);

            @JsProperty
            void setTDigest(AggAllByRequest.ToObjectReturnType0.SpecFieldType.TDigestFieldType tDigest);

            @JsProperty
            void setUnique(AggAllByRequest.ToObjectReturnType0.SpecFieldType.UniqueFieldType unique);

            @JsProperty
            void setWeightedAvg(
                    AggAllByRequest.ToObjectReturnType0.SpecFieldType.WeightedAvgFieldType weightedAvg);

            @JsProperty
            void setWeightedSum(Object weightedSum);
        }

        @JsOverlay
        static AggAllByRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<String> getGroupByColumnsList();

        @JsProperty
        AggAllByRequest.ToObjectReturnType0.ResultIdFieldType getResultId();

        @JsProperty
        AggAllByRequest.ToObjectReturnType0.SourceIdFieldType getSourceId();

        @JsProperty
        AggAllByRequest.ToObjectReturnType0.SpecFieldType getSpec();

        @JsProperty
        void setGroupByColumnsList(JsArray<String> groupByColumnsList);

        @JsOverlay
        default void setGroupByColumnsList(String[] groupByColumnsList) {
            setGroupByColumnsList(Js.<JsArray<String>>uncheckedCast(groupByColumnsList));
        }

        @JsProperty
        void setResultId(AggAllByRequest.ToObjectReturnType0.ResultIdFieldType resultId);

        @JsProperty
        void setSourceId(AggAllByRequest.ToObjectReturnType0.SourceIdFieldType sourceId);

        @JsProperty
        void setSpec(AggAllByRequest.ToObjectReturnType0.SpecFieldType spec);
    }

    public static native AggAllByRequest deserializeBinary(Uint8Array bytes);

    public static native AggAllByRequest deserializeBinaryFromReader(
            AggAllByRequest message, Object reader);

    public static native void serializeBinaryToWriter(AggAllByRequest message, Object writer);

    public static native AggAllByRequest.ToObjectReturnType toObject(
            boolean includeInstance, AggAllByRequest msg);

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

    public native AggAllByRequest.ToObjectReturnType0 toObject();

    public native AggAllByRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
