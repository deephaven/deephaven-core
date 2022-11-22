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
        name = "dhinternal.io.deephaven.proto.table_pb.AggregationRequest",
        namespace = JsPackage.GLOBAL)
public class AggregationRequest {
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
                        static AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.ApproximatePercentileFieldType create() {
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
                        static AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.CountDistinctFieldType create() {
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
                        static AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.DistinctFieldType create() {
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
                        static AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.FormulaFieldType create() {
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
                        static AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.MedianFieldType create() {
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
                        static AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.PercentileFieldType create() {
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
                            static AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType create() {
                                return Js.uncheckedCast(JsPropertyMap.of());
                            }

                            @JsProperty
                            String getColumnName();

                            @JsProperty
                            void setColumnName(String columnName);
                        }

                        @JsOverlay
                        static AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        JsArray<AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType> getColumnsList();

                        @JsOverlay
                        default void setColumnsList(
                                AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType[] columnsList) {
                            setColumnsList(
                                    Js.<JsArray<AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType>>uncheckedCast(
                                            columnsList));
                        }

                        @JsProperty
                        void setColumnsList(
                                JsArray<AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType> columnsList);
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface TDigestFieldType {
                        @JsOverlay
                        static AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.TDigestFieldType create() {
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
                            static AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType create() {
                                return Js.uncheckedCast(JsPropertyMap.of());
                            }

                            @JsProperty
                            double getDoubleValue();

                            @JsProperty
                            double getFloatValue();

                            @JsProperty
                            double getIntValue();

                            @JsProperty
                            String getLongValue();

                            @JsProperty
                            String getStringValue();

                            @JsProperty
                            boolean isBoolValue();

                            @JsProperty
                            void setBoolValue(boolean boolValue);

                            @JsProperty
                            void setDoubleValue(double doubleValue);

                            @JsProperty
                            void setFloatValue(double floatValue);

                            @JsProperty
                            void setIntValue(double intValue);

                            @JsProperty
                            void setLongValue(String longValue);

                            @JsProperty
                            void setStringValue(String stringValue);
                        }

                        @JsOverlay
                        static AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType getNonUniqueSentinel();

                        @JsProperty
                        boolean isIncludeNulls();

                        @JsProperty
                        void setIncludeNulls(boolean includeNulls);

                        @JsProperty
                        void setNonUniqueSentinel(
                                AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType nonUniqueSentinel);
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface WeightedAvgFieldType {
                        @JsOverlay
                        static AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.WeightedAvgFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        String getWeightColumn();

                        @JsProperty
                        void setWeightColumn(String weightColumn);
                    }

                    @JsOverlay
                    static AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    Object getAbsSum();

                    @JsProperty
                    AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.ApproximatePercentileFieldType getApproximatePercentile();

                    @JsProperty
                    Object getAvg();

                    @JsProperty
                    AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.CountDistinctFieldType getCountDistinct();

                    @JsProperty
                    AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.DistinctFieldType getDistinct();

                    @JsProperty
                    Object getFirst();

                    @JsProperty
                    AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.FormulaFieldType getFormula();

                    @JsProperty
                    Object getFreeze();

                    @JsProperty
                    Object getGroup();

                    @JsProperty
                    Object getLast();

                    @JsProperty
                    Object getMax();

                    @JsProperty
                    AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.MedianFieldType getMedian();

                    @JsProperty
                    Object getMin();

                    @JsProperty
                    Object getPb_var();

                    @JsProperty
                    AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.PercentileFieldType getPercentile();

                    @JsProperty
                    AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType getSortedFirst();

                    @JsProperty
                    Object getSortedLast();

                    @JsProperty
                    Object getStd();

                    @JsProperty
                    Object getSum();

                    @JsProperty
                    AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.TDigestFieldType getTDigest();

                    @JsProperty
                    AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType getUnique();

                    @JsProperty
                    AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.WeightedAvgFieldType getWeightedAvg();

                    @JsProperty
                    Object getWeightedSum();

                    @JsProperty
                    void setAbsSum(Object absSum);

                    @JsProperty
                    void setApproximatePercentile(
                            AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.ApproximatePercentileFieldType approximatePercentile);

                    @JsProperty
                    void setAvg(Object avg);

                    @JsProperty
                    void setCountDistinct(
                            AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.CountDistinctFieldType countDistinct);

                    @JsProperty
                    void setDistinct(
                            AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.DistinctFieldType distinct);

                    @JsProperty
                    void setFirst(Object first);

                    @JsProperty
                    void setFormula(
                            AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.FormulaFieldType formula);

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
                            AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.MedianFieldType median);

                    @JsProperty
                    void setMin(Object min);

                    @JsProperty
                    void setPb_var(Object pb_var);

                    @JsProperty
                    void setPercentile(
                            AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.PercentileFieldType percentile);

                    @JsProperty
                    void setSortedFirst(
                            AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType sortedFirst);

                    @JsProperty
                    void setSortedLast(Object sortedLast);

                    @JsProperty
                    void setStd(Object std);

                    @JsProperty
                    void setSum(Object sum);

                    @JsProperty
                    void setTDigest(
                            AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.TDigestFieldType tDigest);

                    @JsProperty
                    void setUnique(
                            AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType unique);

                    @JsProperty
                    void setWeightedAvg(
                            AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.WeightedAvgFieldType weightedAvg);

                    @JsProperty
                    void setWeightedSum(Object weightedSum);
                }

                @JsOverlay
                static AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<String> getMatchPairsList();

                @JsProperty
                AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType getSpec();

                @JsProperty
                void setMatchPairsList(JsArray<String> matchPairsList);

                @JsOverlay
                default void setMatchPairsList(String[] matchPairsList) {
                    setMatchPairsList(Js.<JsArray<String>>uncheckedCast(matchPairsList));
                }

                @JsProperty
                void setSpec(
                        AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType spec);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface CountFieldType {
                @JsOverlay
                static AggregationRequest.ToObjectReturnType.AggregationsListFieldType.CountFieldType create() {
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
                static AggregationRequest.ToObjectReturnType.AggregationsListFieldType.FirstRowKeyFieldType create() {
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
                static AggregationRequest.ToObjectReturnType.AggregationsListFieldType.PartitionFieldType create() {
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
            static AggregationRequest.ToObjectReturnType.AggregationsListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType getColumns();

            @JsProperty
            AggregationRequest.ToObjectReturnType.AggregationsListFieldType.CountFieldType getCount();

            @JsProperty
            AggregationRequest.ToObjectReturnType.AggregationsListFieldType.FirstRowKeyFieldType getFirstRowKey();

            @JsProperty
            Object getLastRowKey();

            @JsProperty
            AggregationRequest.ToObjectReturnType.AggregationsListFieldType.PartitionFieldType getPartition();

            @JsProperty
            void setColumns(
                    AggregationRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType columns);

            @JsProperty
            void setCount(
                    AggregationRequest.ToObjectReturnType.AggregationsListFieldType.CountFieldType count);

            @JsProperty
            void setFirstRowKey(
                    AggregationRequest.ToObjectReturnType.AggregationsListFieldType.FirstRowKeyFieldType firstRowKey);

            @JsProperty
            void setLastRowKey(Object lastRowKey);

            @JsProperty
            void setPartition(
                    AggregationRequest.ToObjectReturnType.AggregationsListFieldType.PartitionFieldType partition);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static AggregationRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType of(
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
            static AggregationRequest.ToObjectReturnType.ResultIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            AggregationRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    AggregationRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<AggregationRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<AggregationRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SourceIdFieldType {
            @JsOverlay
            static AggregationRequest.ToObjectReturnType.SourceIdFieldType create() {
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
        static AggregationRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<AggregationRequest.ToObjectReturnType.AggregationsListFieldType> getAggregationsList();

        @JsProperty
        JsArray<String> getGroupByColumnsList();

        @JsProperty
        Object getInitialGroupsId();

        @JsProperty
        AggregationRequest.ToObjectReturnType.ResultIdFieldType getResultId();

        @JsProperty
        AggregationRequest.ToObjectReturnType.SourceIdFieldType getSourceId();

        @JsProperty
        boolean isPreserveEmpty();

        @JsOverlay
        default void setAggregationsList(
                AggregationRequest.ToObjectReturnType.AggregationsListFieldType[] aggregationsList) {
            setAggregationsList(
                    Js.<JsArray<AggregationRequest.ToObjectReturnType.AggregationsListFieldType>>uncheckedCast(
                            aggregationsList));
        }

        @JsProperty
        void setAggregationsList(
                JsArray<AggregationRequest.ToObjectReturnType.AggregationsListFieldType> aggregationsList);

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
        void setResultId(AggregationRequest.ToObjectReturnType.ResultIdFieldType resultId);

        @JsProperty
        void setSourceId(AggregationRequest.ToObjectReturnType.SourceIdFieldType sourceId);
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
                        static AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.ApproximatePercentileFieldType create() {
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
                        static AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.CountDistinctFieldType create() {
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
                        static AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.DistinctFieldType create() {
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
                        static AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.FormulaFieldType create() {
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
                        static AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.MedianFieldType create() {
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
                        static AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.PercentileFieldType create() {
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
                            static AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType create() {
                                return Js.uncheckedCast(JsPropertyMap.of());
                            }

                            @JsProperty
                            String getColumnName();

                            @JsProperty
                            void setColumnName(String columnName);
                        }

                        @JsOverlay
                        static AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        JsArray<AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType> getColumnsList();

                        @JsOverlay
                        default void setColumnsList(
                                AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType[] columnsList) {
                            setColumnsList(
                                    Js.<JsArray<AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType>>uncheckedCast(
                                            columnsList));
                        }

                        @JsProperty
                        void setColumnsList(
                                JsArray<AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType> columnsList);
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface TDigestFieldType {
                        @JsOverlay
                        static AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.TDigestFieldType create() {
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
                            static AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType create() {
                                return Js.uncheckedCast(JsPropertyMap.of());
                            }

                            @JsProperty
                            double getDoubleValue();

                            @JsProperty
                            double getFloatValue();

                            @JsProperty
                            double getIntValue();

                            @JsProperty
                            String getLongValue();

                            @JsProperty
                            String getStringValue();

                            @JsProperty
                            boolean isBoolValue();

                            @JsProperty
                            void setBoolValue(boolean boolValue);

                            @JsProperty
                            void setDoubleValue(double doubleValue);

                            @JsProperty
                            void setFloatValue(double floatValue);

                            @JsProperty
                            void setIntValue(double intValue);

                            @JsProperty
                            void setLongValue(String longValue);

                            @JsProperty
                            void setStringValue(String stringValue);
                        }

                        @JsOverlay
                        static AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType getNonUniqueSentinel();

                        @JsProperty
                        boolean isIncludeNulls();

                        @JsProperty
                        void setIncludeNulls(boolean includeNulls);

                        @JsProperty
                        void setNonUniqueSentinel(
                                AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType nonUniqueSentinel);
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface WeightedAvgFieldType {
                        @JsOverlay
                        static AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.WeightedAvgFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        String getWeightColumn();

                        @JsProperty
                        void setWeightColumn(String weightColumn);
                    }

                    @JsOverlay
                    static AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    Object getAbsSum();

                    @JsProperty
                    AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.ApproximatePercentileFieldType getApproximatePercentile();

                    @JsProperty
                    Object getAvg();

                    @JsProperty
                    AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.CountDistinctFieldType getCountDistinct();

                    @JsProperty
                    AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.DistinctFieldType getDistinct();

                    @JsProperty
                    Object getFirst();

                    @JsProperty
                    AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.FormulaFieldType getFormula();

                    @JsProperty
                    Object getFreeze();

                    @JsProperty
                    Object getGroup();

                    @JsProperty
                    Object getLast();

                    @JsProperty
                    Object getMax();

                    @JsProperty
                    AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.MedianFieldType getMedian();

                    @JsProperty
                    Object getMin();

                    @JsProperty
                    Object getPb_var();

                    @JsProperty
                    AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.PercentileFieldType getPercentile();

                    @JsProperty
                    AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType getSortedFirst();

                    @JsProperty
                    Object getSortedLast();

                    @JsProperty
                    Object getStd();

                    @JsProperty
                    Object getSum();

                    @JsProperty
                    AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.TDigestFieldType getTDigest();

                    @JsProperty
                    AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType getUnique();

                    @JsProperty
                    AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.WeightedAvgFieldType getWeightedAvg();

                    @JsProperty
                    Object getWeightedSum();

                    @JsProperty
                    void setAbsSum(Object absSum);

                    @JsProperty
                    void setApproximatePercentile(
                            AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.ApproximatePercentileFieldType approximatePercentile);

                    @JsProperty
                    void setAvg(Object avg);

                    @JsProperty
                    void setCountDistinct(
                            AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.CountDistinctFieldType countDistinct);

                    @JsProperty
                    void setDistinct(
                            AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.DistinctFieldType distinct);

                    @JsProperty
                    void setFirst(Object first);

                    @JsProperty
                    void setFormula(
                            AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.FormulaFieldType formula);

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
                            AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.MedianFieldType median);

                    @JsProperty
                    void setMin(Object min);

                    @JsProperty
                    void setPb_var(Object pb_var);

                    @JsProperty
                    void setPercentile(
                            AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.PercentileFieldType percentile);

                    @JsProperty
                    void setSortedFirst(
                            AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType sortedFirst);

                    @JsProperty
                    void setSortedLast(Object sortedLast);

                    @JsProperty
                    void setStd(Object std);

                    @JsProperty
                    void setSum(Object sum);

                    @JsProperty
                    void setTDigest(
                            AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.TDigestFieldType tDigest);

                    @JsProperty
                    void setUnique(
                            AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType unique);

                    @JsProperty
                    void setWeightedAvg(
                            AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.WeightedAvgFieldType weightedAvg);

                    @JsProperty
                    void setWeightedSum(Object weightedSum);
                }

                @JsOverlay
                static AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<String> getMatchPairsList();

                @JsProperty
                AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType getSpec();

                @JsProperty
                void setMatchPairsList(JsArray<String> matchPairsList);

                @JsOverlay
                default void setMatchPairsList(String[] matchPairsList) {
                    setMatchPairsList(Js.<JsArray<String>>uncheckedCast(matchPairsList));
                }

                @JsProperty
                void setSpec(
                        AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType spec);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface CountFieldType {
                @JsOverlay
                static AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.CountFieldType create() {
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
                static AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.FirstRowKeyFieldType create() {
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
                static AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.PartitionFieldType create() {
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
            static AggregationRequest.ToObjectReturnType0.AggregationsListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType getColumns();

            @JsProperty
            AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.CountFieldType getCount();

            @JsProperty
            AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.FirstRowKeyFieldType getFirstRowKey();

            @JsProperty
            Object getLastRowKey();

            @JsProperty
            AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.PartitionFieldType getPartition();

            @JsProperty
            void setColumns(
                    AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType columns);

            @JsProperty
            void setCount(
                    AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.CountFieldType count);

            @JsProperty
            void setFirstRowKey(
                    AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.FirstRowKeyFieldType firstRowKey);

            @JsProperty
            void setLastRowKey(Object lastRowKey);

            @JsProperty
            void setPartition(
                    AggregationRequest.ToObjectReturnType0.AggregationsListFieldType.PartitionFieldType partition);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static AggregationRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType of(
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
            static AggregationRequest.ToObjectReturnType0.ResultIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            AggregationRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    AggregationRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<AggregationRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<AggregationRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SourceIdFieldType {
            @JsOverlay
            static AggregationRequest.ToObjectReturnType0.SourceIdFieldType create() {
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
        static AggregationRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<AggregationRequest.ToObjectReturnType0.AggregationsListFieldType> getAggregationsList();

        @JsProperty
        JsArray<String> getGroupByColumnsList();

        @JsProperty
        Object getInitialGroupsId();

        @JsProperty
        AggregationRequest.ToObjectReturnType0.ResultIdFieldType getResultId();

        @JsProperty
        AggregationRequest.ToObjectReturnType0.SourceIdFieldType getSourceId();

        @JsProperty
        boolean isPreserveEmpty();

        @JsOverlay
        default void setAggregationsList(
                AggregationRequest.ToObjectReturnType0.AggregationsListFieldType[] aggregationsList) {
            setAggregationsList(
                    Js.<JsArray<AggregationRequest.ToObjectReturnType0.AggregationsListFieldType>>uncheckedCast(
                            aggregationsList));
        }

        @JsProperty
        void setAggregationsList(
                JsArray<AggregationRequest.ToObjectReturnType0.AggregationsListFieldType> aggregationsList);

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
        void setResultId(AggregationRequest.ToObjectReturnType0.ResultIdFieldType resultId);

        @JsProperty
        void setSourceId(AggregationRequest.ToObjectReturnType0.SourceIdFieldType sourceId);
    }

    public static native AggregationRequest deserializeBinary(Uint8Array bytes);

    public static native AggregationRequest deserializeBinaryFromReader(
            AggregationRequest message, Object reader);

    public static native void serializeBinaryToWriter(AggregationRequest message, Object writer);

    public static native AggregationRequest.ToObjectReturnType toObject(
            boolean includeInstance, AggregationRequest msg);

    public native Aggregation addAggregations();

    public native Aggregation addAggregations(Aggregation value, double index);

    public native Aggregation addAggregations(Aggregation value);

    public native String addGroupByColumns(String value, double index);

    public native String addGroupByColumns(String value);

    public native void clearAggregationsList();

    public native void clearGroupByColumnsList();

    public native void clearInitialGroupsId();

    public native void clearPreserveEmpty();

    public native void clearResultId();

    public native void clearSourceId();

    public native JsArray<Aggregation> getAggregationsList();

    public native JsArray<String> getGroupByColumnsList();

    public native TableReference getInitialGroupsId();

    public native boolean getPreserveEmpty();

    public native Ticket getResultId();

    public native TableReference getSourceId();

    public native boolean hasInitialGroupsId();

    public native boolean hasPreserveEmpty();

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

    public native AggregationRequest.ToObjectReturnType0 toObject();

    public native AggregationRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
