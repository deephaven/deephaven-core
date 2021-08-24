package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.batchtablerequest.Operation;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.table_pb.BatchTableRequest",
    namespace = JsPackage.GLOBAL)
public class BatchTableRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface OpsListFieldType {
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
                            setOptionalReferencesList(
                                Js.<JsArray<Object>>uncheckedCast(optionalReferencesList));
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
                    setGroupByColumnSpecsList(
                        Js.<JsArray<String>>uncheckedCast(groupByColumnSpecsList));
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
                Object getLeftId();

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getRightId();

                @JsProperty
                JsArray<String> getStampColumnsList();

                @JsProperty
                boolean isDoInitialSnapshot();

                @JsProperty
                void setDoInitialSnapshot(boolean doInitialSnapshot);

                @JsProperty
                void setLeftId(Object leftId);

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setRightId(Object rightId);

                @JsProperty
                void setStampColumnsList(JsArray<String> stampColumnsList);

                @JsOverlay
                default void setStampColumnsList(String[] stampColumnsList) {
                    setStampColumnsList(Js.<JsArray<String>>uncheckedCast(stampColumnsList));
                }
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
                    setColumnsToUngroupList(
                        Js.<JsArray<String>>uncheckedCast(columnsToUngroupList));
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

            @JsOverlay
            static BatchTableRequest.ToObjectReturnType.OpsListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            BatchTableRequest.ToObjectReturnType.OpsListFieldType.AsOfJoinFieldType getAsOfJoin();

            @JsProperty
            BatchTableRequest.ToObjectReturnType.OpsListFieldType.ComboAggregateFieldType getComboAggregate();

            @JsProperty
            BatchTableRequest.ToObjectReturnType.OpsListFieldType.CrossJoinFieldType getCrossJoin();

            @JsProperty
            BatchTableRequest.ToObjectReturnType.OpsListFieldType.DropColumnsFieldType getDropColumns();

            @JsProperty
            BatchTableRequest.ToObjectReturnType.OpsListFieldType.EmptyTableFieldType getEmptyTable();

            @JsProperty
            BatchTableRequest.ToObjectReturnType.OpsListFieldType.ExactJoinFieldType getExactJoin();

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
            Object getUpdateView();

            @JsProperty
            Object getView();

            @JsProperty
            void setAsOfJoin(
                BatchTableRequest.ToObjectReturnType.OpsListFieldType.AsOfJoinFieldType asOfJoin);

            @JsProperty
            void setComboAggregate(
                BatchTableRequest.ToObjectReturnType.OpsListFieldType.ComboAggregateFieldType comboAggregate);

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
            void setFilter(
                BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType filter);

            @JsProperty
            void setFlatten(
                BatchTableRequest.ToObjectReturnType.OpsListFieldType.FlattenFieldType flatten);

            @JsProperty
            void setHead(BatchTableRequest.ToObjectReturnType.OpsListFieldType.HeadFieldType head);

            @JsProperty
            void setHeadBy(
                BatchTableRequest.ToObjectReturnType.OpsListFieldType.HeadByFieldType headBy);

            @JsProperty
            void setLazyUpdate(Object lazyUpdate);

            @JsProperty
            void setLeftJoin(
                BatchTableRequest.ToObjectReturnType.OpsListFieldType.LeftJoinFieldType leftJoin);

            @JsProperty
            void setMerge(
                BatchTableRequest.ToObjectReturnType.OpsListFieldType.MergeFieldType merge);

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
            void setUpdate(
                BatchTableRequest.ToObjectReturnType.OpsListFieldType.UpdateFieldType update);

            @JsProperty
            void setUpdateView(Object updateView);

            @JsProperty
            void setView(Object view);
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
                            setOptionalReferencesList(
                                Js.<JsArray<Object>>uncheckedCast(optionalReferencesList));
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
                    setGroupByColumnSpecsList(
                        Js.<JsArray<String>>uncheckedCast(groupByColumnSpecsList));
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
                Object getLeftId();

                @JsProperty
                Object getResultId();

                @JsProperty
                Object getRightId();

                @JsProperty
                JsArray<String> getStampColumnsList();

                @JsProperty
                boolean isDoInitialSnapshot();

                @JsProperty
                void setDoInitialSnapshot(boolean doInitialSnapshot);

                @JsProperty
                void setLeftId(Object leftId);

                @JsProperty
                void setResultId(Object resultId);

                @JsProperty
                void setRightId(Object rightId);

                @JsProperty
                void setStampColumnsList(JsArray<String> stampColumnsList);

                @JsOverlay
                default void setStampColumnsList(String[] stampColumnsList) {
                    setStampColumnsList(Js.<JsArray<String>>uncheckedCast(stampColumnsList));
                }
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
                    setColumnsToUngroupList(
                        Js.<JsArray<String>>uncheckedCast(columnsToUngroupList));
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

            @JsOverlay
            static BatchTableRequest.ToObjectReturnType0.OpsListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AsOfJoinFieldType getAsOfJoin();

            @JsProperty
            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.ComboAggregateFieldType getComboAggregate();

            @JsProperty
            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.CrossJoinFieldType getCrossJoin();

            @JsProperty
            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.DropColumnsFieldType getDropColumns();

            @JsProperty
            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.EmptyTableFieldType getEmptyTable();

            @JsProperty
            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.ExactJoinFieldType getExactJoin();

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
            Object getUpdateView();

            @JsProperty
            Object getView();

            @JsProperty
            void setAsOfJoin(
                BatchTableRequest.ToObjectReturnType0.OpsListFieldType.AsOfJoinFieldType asOfJoin);

            @JsProperty
            void setComboAggregate(
                BatchTableRequest.ToObjectReturnType0.OpsListFieldType.ComboAggregateFieldType comboAggregate);

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
            void setFilter(
                BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType filter);

            @JsProperty
            void setFlatten(
                BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FlattenFieldType flatten);

            @JsProperty
            void setHead(BatchTableRequest.ToObjectReturnType0.OpsListFieldType.HeadFieldType head);

            @JsProperty
            void setHeadBy(
                BatchTableRequest.ToObjectReturnType0.OpsListFieldType.HeadByFieldType headBy);

            @JsProperty
            void setLazyUpdate(Object lazyUpdate);

            @JsProperty
            void setLeftJoin(
                BatchTableRequest.ToObjectReturnType0.OpsListFieldType.LeftJoinFieldType leftJoin);

            @JsProperty
            void setMerge(
                BatchTableRequest.ToObjectReturnType0.OpsListFieldType.MergeFieldType merge);

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
            void setUpdate(
                BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UpdateFieldType update);

            @JsProperty
            void setUpdateView(Object updateView);

            @JsProperty
            void setView(Object view);
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
