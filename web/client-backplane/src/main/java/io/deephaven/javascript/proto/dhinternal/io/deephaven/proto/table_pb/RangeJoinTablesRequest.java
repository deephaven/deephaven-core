package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.rangejointablesrequest.RangeEndRuleMap;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.rangejointablesrequest.RangeStartRuleMap;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.Ticket;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.RangeJoinTablesRequest",
        namespace = JsPackage.GLOBAL)
public class RangeJoinTablesRequest {
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
                        static RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.ApproximatePercentileFieldType create() {
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
                        static RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.CountDistinctFieldType create() {
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
                        static RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.DistinctFieldType create() {
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
                        static RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.FormulaFieldType create() {
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
                        static RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.MedianFieldType create() {
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
                        static RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.PercentileFieldType create() {
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
                            static RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType create() {
                                return Js.uncheckedCast(JsPropertyMap.of());
                            }

                            @JsProperty
                            String getColumnName();

                            @JsProperty
                            void setColumnName(String columnName);
                        }

                        @JsOverlay
                        static RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        JsArray<RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType> getColumnsList();

                        @JsOverlay
                        default void setColumnsList(
                                RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType[] columnsList) {
                            setColumnsList(
                                    Js.<JsArray<RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType>>uncheckedCast(
                                            columnsList));
                        }

                        @JsProperty
                        void setColumnsList(
                                JsArray<RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType> columnsList);
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface TDigestFieldType {
                        @JsOverlay
                        static RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.TDigestFieldType create() {
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
                            static RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType create() {
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
                        static RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType getNonUniqueSentinel();

                        @JsProperty
                        boolean isIncludeNulls();

                        @JsProperty
                        void setIncludeNulls(boolean includeNulls);

                        @JsProperty
                        void setNonUniqueSentinel(
                                RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType nonUniqueSentinel);
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface WeightedAvgFieldType {
                        @JsOverlay
                        static RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.WeightedAvgFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        String getWeightColumn();

                        @JsProperty
                        void setWeightColumn(String weightColumn);
                    }

                    @JsOverlay
                    static RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    Object getAbsSum();

                    @JsProperty
                    RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.ApproximatePercentileFieldType getApproximatePercentile();

                    @JsProperty
                    Object getAvg();

                    @JsProperty
                    RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.CountDistinctFieldType getCountDistinct();

                    @JsProperty
                    RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.DistinctFieldType getDistinct();

                    @JsProperty
                    Object getFirst();

                    @JsProperty
                    RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.FormulaFieldType getFormula();

                    @JsProperty
                    Object getFreeze();

                    @JsProperty
                    Object getGroup();

                    @JsProperty
                    Object getLast();

                    @JsProperty
                    Object getMax();

                    @JsProperty
                    RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.MedianFieldType getMedian();

                    @JsProperty
                    Object getMin();

                    @JsProperty
                    Object getPb_var();

                    @JsProperty
                    RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.PercentileFieldType getPercentile();

                    @JsProperty
                    RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType getSortedFirst();

                    @JsProperty
                    Object getSortedLast();

                    @JsProperty
                    Object getStd();

                    @JsProperty
                    Object getSum();

                    @JsProperty
                    RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.TDigestFieldType getTDigest();

                    @JsProperty
                    RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType getUnique();

                    @JsProperty
                    RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.WeightedAvgFieldType getWeightedAvg();

                    @JsProperty
                    Object getWeightedSum();

                    @JsProperty
                    void setAbsSum(Object absSum);

                    @JsProperty
                    void setApproximatePercentile(
                            RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.ApproximatePercentileFieldType approximatePercentile);

                    @JsProperty
                    void setAvg(Object avg);

                    @JsProperty
                    void setCountDistinct(
                            RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.CountDistinctFieldType countDistinct);

                    @JsProperty
                    void setDistinct(
                            RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.DistinctFieldType distinct);

                    @JsProperty
                    void setFirst(Object first);

                    @JsProperty
                    void setFormula(
                            RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.FormulaFieldType formula);

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
                            RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.MedianFieldType median);

                    @JsProperty
                    void setMin(Object min);

                    @JsProperty
                    void setPb_var(Object pb_var);

                    @JsProperty
                    void setPercentile(
                            RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.PercentileFieldType percentile);

                    @JsProperty
                    void setSortedFirst(
                            RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType sortedFirst);

                    @JsProperty
                    void setSortedLast(Object sortedLast);

                    @JsProperty
                    void setStd(Object std);

                    @JsProperty
                    void setSum(Object sum);

                    @JsProperty
                    void setTDigest(
                            RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.TDigestFieldType tDigest);

                    @JsProperty
                    void setUnique(
                            RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType unique);

                    @JsProperty
                    void setWeightedAvg(
                            RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.WeightedAvgFieldType weightedAvg);

                    @JsProperty
                    void setWeightedSum(Object weightedSum);
                }

                @JsOverlay
                static RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<String> getMatchPairsList();

                @JsProperty
                RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType getSpec();

                @JsProperty
                void setMatchPairsList(JsArray<String> matchPairsList);

                @JsOverlay
                default void setMatchPairsList(String[] matchPairsList) {
                    setMatchPairsList(Js.<JsArray<String>>uncheckedCast(matchPairsList));
                }

                @JsProperty
                void setSpec(
                        RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType.SpecFieldType spec);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface CountFieldType {
                @JsOverlay
                static RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.CountFieldType create() {
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
                static RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.FirstRowKeyFieldType create() {
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
                static RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.PartitionFieldType create() {
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
            static RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType getColumns();

            @JsProperty
            RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.CountFieldType getCount();

            @JsProperty
            RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.FirstRowKeyFieldType getFirstRowKey();

            @JsProperty
            Object getLastRowKey();

            @JsProperty
            RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.PartitionFieldType getPartition();

            @JsProperty
            void setColumns(
                    RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.ColumnsFieldType columns);

            @JsProperty
            void setCount(
                    RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.CountFieldType count);

            @JsProperty
            void setFirstRowKey(
                    RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.FirstRowKeyFieldType firstRowKey);

            @JsProperty
            void setLastRowKey(Object lastRowKey);

            @JsProperty
            void setPartition(
                    RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType.PartitionFieldType partition);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface LeftIdFieldType {
            @JsOverlay
            static RangeJoinTablesRequest.ToObjectReturnType.LeftIdFieldType create() {
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
        public interface ResultIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static RangeJoinTablesRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType of(
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
            static RangeJoinTablesRequest.ToObjectReturnType.ResultIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            RangeJoinTablesRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    RangeJoinTablesRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<RangeJoinTablesRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<RangeJoinTablesRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsOverlay
        static RangeJoinTablesRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType> getAggregationsList();

        @JsProperty
        JsArray<String> getExactMatchColumnsList();

        @JsProperty
        String getLeftEndColumn();

        @JsProperty
        RangeJoinTablesRequest.ToObjectReturnType.LeftIdFieldType getLeftId();

        @JsProperty
        String getLeftStartColumn();

        @JsProperty
        double getRangeEndRule();

        @JsProperty
        double getRangeStartRule();

        @JsProperty
        RangeJoinTablesRequest.ToObjectReturnType.ResultIdFieldType getResultId();

        @JsProperty
        Object getRightId();

        @JsProperty
        String getRightRangeColumn();

        @JsOverlay
        default void setAggregationsList(
                RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType[] aggregationsList) {
            setAggregationsList(
                    Js.<JsArray<RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType>>uncheckedCast(
                            aggregationsList));
        }

        @JsProperty
        void setAggregationsList(
                JsArray<RangeJoinTablesRequest.ToObjectReturnType.AggregationsListFieldType> aggregationsList);

        @JsProperty
        void setExactMatchColumnsList(JsArray<String> exactMatchColumnsList);

        @JsOverlay
        default void setExactMatchColumnsList(String[] exactMatchColumnsList) {
            setExactMatchColumnsList(Js.<JsArray<String>>uncheckedCast(exactMatchColumnsList));
        }

        @JsProperty
        void setLeftEndColumn(String leftEndColumn);

        @JsProperty
        void setLeftId(RangeJoinTablesRequest.ToObjectReturnType.LeftIdFieldType leftId);

        @JsProperty
        void setLeftStartColumn(String leftStartColumn);

        @JsProperty
        void setRangeEndRule(double rangeEndRule);

        @JsProperty
        void setRangeStartRule(double rangeStartRule);

        @JsProperty
        void setResultId(RangeJoinTablesRequest.ToObjectReturnType.ResultIdFieldType resultId);

        @JsProperty
        void setRightId(Object rightId);

        @JsProperty
        void setRightRangeColumn(String rightRangeColumn);
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
                        static RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.ApproximatePercentileFieldType create() {
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
                        static RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.CountDistinctFieldType create() {
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
                        static RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.DistinctFieldType create() {
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
                        static RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.FormulaFieldType create() {
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
                        static RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.MedianFieldType create() {
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
                        static RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.PercentileFieldType create() {
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
                            static RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType create() {
                                return Js.uncheckedCast(JsPropertyMap.of());
                            }

                            @JsProperty
                            String getColumnName();

                            @JsProperty
                            void setColumnName(String columnName);
                        }

                        @JsOverlay
                        static RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        JsArray<RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType> getColumnsList();

                        @JsOverlay
                        default void setColumnsList(
                                RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType[] columnsList) {
                            setColumnsList(
                                    Js.<JsArray<RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType>>uncheckedCast(
                                            columnsList));
                        }

                        @JsProperty
                        void setColumnsList(
                                JsArray<RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType.ColumnsListFieldType> columnsList);
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface TDigestFieldType {
                        @JsOverlay
                        static RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.TDigestFieldType create() {
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
                            static RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType create() {
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
                        static RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType getNonUniqueSentinel();

                        @JsProperty
                        boolean isIncludeNulls();

                        @JsProperty
                        void setIncludeNulls(boolean includeNulls);

                        @JsProperty
                        void setNonUniqueSentinel(
                                RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType.NonUniqueSentinelFieldType nonUniqueSentinel);
                    }

                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface WeightedAvgFieldType {
                        @JsOverlay
                        static RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.WeightedAvgFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        String getWeightColumn();

                        @JsProperty
                        void setWeightColumn(String weightColumn);
                    }

                    @JsOverlay
                    static RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    Object getAbsSum();

                    @JsProperty
                    RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.ApproximatePercentileFieldType getApproximatePercentile();

                    @JsProperty
                    Object getAvg();

                    @JsProperty
                    RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.CountDistinctFieldType getCountDistinct();

                    @JsProperty
                    RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.DistinctFieldType getDistinct();

                    @JsProperty
                    Object getFirst();

                    @JsProperty
                    RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.FormulaFieldType getFormula();

                    @JsProperty
                    Object getFreeze();

                    @JsProperty
                    Object getGroup();

                    @JsProperty
                    Object getLast();

                    @JsProperty
                    Object getMax();

                    @JsProperty
                    RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.MedianFieldType getMedian();

                    @JsProperty
                    Object getMin();

                    @JsProperty
                    Object getPb_var();

                    @JsProperty
                    RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.PercentileFieldType getPercentile();

                    @JsProperty
                    RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType getSortedFirst();

                    @JsProperty
                    Object getSortedLast();

                    @JsProperty
                    Object getStd();

                    @JsProperty
                    Object getSum();

                    @JsProperty
                    RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.TDigestFieldType getTDigest();

                    @JsProperty
                    RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType getUnique();

                    @JsProperty
                    RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.WeightedAvgFieldType getWeightedAvg();

                    @JsProperty
                    Object getWeightedSum();

                    @JsProperty
                    void setAbsSum(Object absSum);

                    @JsProperty
                    void setApproximatePercentile(
                            RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.ApproximatePercentileFieldType approximatePercentile);

                    @JsProperty
                    void setAvg(Object avg);

                    @JsProperty
                    void setCountDistinct(
                            RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.CountDistinctFieldType countDistinct);

                    @JsProperty
                    void setDistinct(
                            RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.DistinctFieldType distinct);

                    @JsProperty
                    void setFirst(Object first);

                    @JsProperty
                    void setFormula(
                            RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.FormulaFieldType formula);

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
                            RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.MedianFieldType median);

                    @JsProperty
                    void setMin(Object min);

                    @JsProperty
                    void setPb_var(Object pb_var);

                    @JsProperty
                    void setPercentile(
                            RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.PercentileFieldType percentile);

                    @JsProperty
                    void setSortedFirst(
                            RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.SortedFirstFieldType sortedFirst);

                    @JsProperty
                    void setSortedLast(Object sortedLast);

                    @JsProperty
                    void setStd(Object std);

                    @JsProperty
                    void setSum(Object sum);

                    @JsProperty
                    void setTDigest(
                            RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.TDigestFieldType tDigest);

                    @JsProperty
                    void setUnique(
                            RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.UniqueFieldType unique);

                    @JsProperty
                    void setWeightedAvg(
                            RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType.WeightedAvgFieldType weightedAvg);

                    @JsProperty
                    void setWeightedSum(Object weightedSum);
                }

                @JsOverlay
                static RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<String> getMatchPairsList();

                @JsProperty
                RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType getSpec();

                @JsProperty
                void setMatchPairsList(JsArray<String> matchPairsList);

                @JsOverlay
                default void setMatchPairsList(String[] matchPairsList) {
                    setMatchPairsList(Js.<JsArray<String>>uncheckedCast(matchPairsList));
                }

                @JsProperty
                void setSpec(
                        RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType.SpecFieldType spec);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface CountFieldType {
                @JsOverlay
                static RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.CountFieldType create() {
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
                static RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.FirstRowKeyFieldType create() {
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
                static RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.PartitionFieldType create() {
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
            static RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType getColumns();

            @JsProperty
            RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.CountFieldType getCount();

            @JsProperty
            RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.FirstRowKeyFieldType getFirstRowKey();

            @JsProperty
            Object getLastRowKey();

            @JsProperty
            RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.PartitionFieldType getPartition();

            @JsProperty
            void setColumns(
                    RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.ColumnsFieldType columns);

            @JsProperty
            void setCount(
                    RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.CountFieldType count);

            @JsProperty
            void setFirstRowKey(
                    RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.FirstRowKeyFieldType firstRowKey);

            @JsProperty
            void setLastRowKey(Object lastRowKey);

            @JsProperty
            void setPartition(
                    RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType.PartitionFieldType partition);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface LeftIdFieldType {
            @JsOverlay
            static RangeJoinTablesRequest.ToObjectReturnType0.LeftIdFieldType create() {
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
        public interface ResultIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static RangeJoinTablesRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType of(
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
            static RangeJoinTablesRequest.ToObjectReturnType0.ResultIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            RangeJoinTablesRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    RangeJoinTablesRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<RangeJoinTablesRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<RangeJoinTablesRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsOverlay
        static RangeJoinTablesRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType> getAggregationsList();

        @JsProperty
        JsArray<String> getExactMatchColumnsList();

        @JsProperty
        String getLeftEndColumn();

        @JsProperty
        RangeJoinTablesRequest.ToObjectReturnType0.LeftIdFieldType getLeftId();

        @JsProperty
        String getLeftStartColumn();

        @JsProperty
        double getRangeEndRule();

        @JsProperty
        double getRangeStartRule();

        @JsProperty
        RangeJoinTablesRequest.ToObjectReturnType0.ResultIdFieldType getResultId();

        @JsProperty
        Object getRightId();

        @JsProperty
        String getRightRangeColumn();

        @JsOverlay
        default void setAggregationsList(
                RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType[] aggregationsList) {
            setAggregationsList(
                    Js.<JsArray<RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType>>uncheckedCast(
                            aggregationsList));
        }

        @JsProperty
        void setAggregationsList(
                JsArray<RangeJoinTablesRequest.ToObjectReturnType0.AggregationsListFieldType> aggregationsList);

        @JsProperty
        void setExactMatchColumnsList(JsArray<String> exactMatchColumnsList);

        @JsOverlay
        default void setExactMatchColumnsList(String[] exactMatchColumnsList) {
            setExactMatchColumnsList(Js.<JsArray<String>>uncheckedCast(exactMatchColumnsList));
        }

        @JsProperty
        void setLeftEndColumn(String leftEndColumn);

        @JsProperty
        void setLeftId(RangeJoinTablesRequest.ToObjectReturnType0.LeftIdFieldType leftId);

        @JsProperty
        void setLeftStartColumn(String leftStartColumn);

        @JsProperty
        void setRangeEndRule(double rangeEndRule);

        @JsProperty
        void setRangeStartRule(double rangeStartRule);

        @JsProperty
        void setResultId(RangeJoinTablesRequest.ToObjectReturnType0.ResultIdFieldType resultId);

        @JsProperty
        void setRightId(Object rightId);

        @JsProperty
        void setRightRangeColumn(String rightRangeColumn);
    }

    public static RangeEndRuleMap RangeEndRule;
    public static RangeStartRuleMap RangeStartRule;

    public static native RangeJoinTablesRequest deserializeBinary(Uint8Array bytes);

    public static native RangeJoinTablesRequest deserializeBinaryFromReader(
            RangeJoinTablesRequest message, Object reader);

    public static native void serializeBinaryToWriter(RangeJoinTablesRequest message, Object writer);

    public static native RangeJoinTablesRequest.ToObjectReturnType toObject(
            boolean includeInstance, RangeJoinTablesRequest msg);

    public native Aggregation addAggregations();

    public native Aggregation addAggregations(Aggregation value, double index);

    public native Aggregation addAggregations(Aggregation value);

    public native String addExactMatchColumns(String value, double index);

    public native String addExactMatchColumns(String value);

    public native void clearAggregationsList();

    public native void clearExactMatchColumnsList();

    public native void clearLeftId();

    public native void clearResultId();

    public native void clearRightId();

    public native JsArray<Aggregation> getAggregationsList();

    public native JsArray<String> getExactMatchColumnsList();

    public native String getLeftEndColumn();

    public native TableReference getLeftId();

    public native String getLeftStartColumn();

    public native double getRangeEndRule();

    public native double getRangeStartRule();

    public native Ticket getResultId();

    public native TableReference getRightId();

    public native String getRightRangeColumn();

    public native boolean hasLeftId();

    public native boolean hasResultId();

    public native boolean hasRightId();

    public native Uint8Array serializeBinary();

    @JsOverlay
    public final void setAggregationsList(Aggregation[] value) {
        setAggregationsList(Js.<JsArray<Aggregation>>uncheckedCast(value));
    }

    public native void setAggregationsList(JsArray<Aggregation> value);

    public native void setExactMatchColumnsList(JsArray<String> value);

    @JsOverlay
    public final void setExactMatchColumnsList(String[] value) {
        setExactMatchColumnsList(Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void setLeftEndColumn(String value);

    public native void setLeftId();

    public native void setLeftId(TableReference value);

    public native void setLeftStartColumn(String value);

    public native void setRangeEndRule(double value);

    public native void setRangeStartRule(double value);

    public native void setResultId();

    public native void setResultId(Ticket value);

    public native void setRightId();

    public native void setRightId(TableReference value);

    public native void setRightRangeColumn(String value);

    public native RangeJoinTablesRequest.ToObjectReturnType0 toObject();

    public native RangeJoinTablesRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
