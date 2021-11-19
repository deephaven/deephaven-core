package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.batchtablerequest;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.ApplyPreviewColumnsRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.AsOfJoinTablesRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.ComboAggregateRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.CreateInputTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.CrossJoinTablesRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.DropColumnsRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.EmptyTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.ExactJoinTablesRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.FetchPandasTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.FetchTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.FilterTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.FlattenRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.HeadOrTailByRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.HeadOrTailRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.LeftJoinTablesRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.MergeTablesRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.NaturalJoinTablesRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.RunChartDownsampleRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.SelectDistinctRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.SelectOrUpdateRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.SnapshotTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.SortTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.TimeTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.UngroupRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.UnstructuredFilterTableRequest;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.BatchTableRequest.Operation",
        namespace = JsPackage.GLOBAL)
public class Operation {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
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
        public interface FetchPandasTableFieldType {
            @JsOverlay
            static Operation.ToObjectReturnType.FetchPandasTableFieldType create() {
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

        @JsOverlay
        static Operation.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

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
        Operation.ToObjectReturnType.FetchPandasTableFieldType getFetchPandasTable();

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
        Object getUpdateView();

        @JsProperty
        Object getView();

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
        void setFetchPandasTable(
                Operation.ToObjectReturnType.FetchPandasTableFieldType fetchPandasTable);

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
        void setUpdateView(Object updateView);

        @JsProperty
        void setView(Object view);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
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
        public interface FetchPandasTableFieldType {
            @JsOverlay
            static Operation.ToObjectReturnType0.FetchPandasTableFieldType create() {
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

        @JsOverlay
        static Operation.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

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
        Operation.ToObjectReturnType0.FetchPandasTableFieldType getFetchPandasTable();

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
        Object getUpdateView();

        @JsProperty
        Object getView();

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
        void setFetchPandasTable(
                Operation.ToObjectReturnType0.FetchPandasTableFieldType fetchPandasTable);

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
        void setUpdateView(Object updateView);

        @JsProperty
        void setView(Object view);
    }

    public static native Operation deserializeBinary(Uint8Array bytes);

    public static native Operation deserializeBinaryFromReader(Operation message, Object reader);

    public static native void serializeBinaryToWriter(Operation message, Object writer);

    public static native Operation.ToObjectReturnType toObject(
            boolean includeInstance, Operation msg);

    public native void clearApplyPreviewColumns();

    public native void clearAsOfJoin();

    public native void clearComboAggregate();

    public native void clearCreateInputTable();

    public native void clearCrossJoin();

    public native void clearDropColumns();

    public native void clearEmptyTable();

    public native void clearExactJoin();

    public native void clearFetchPandasTable();

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

    public native void clearSort();

    public native void clearTail();

    public native void clearTailBy();

    public native void clearTimeTable();

    public native void clearUngroup();

    public native void clearUnstructuredFilter();

    public native void clearUpdate();

    public native void clearUpdateView();

    public native void clearView();

    public native ApplyPreviewColumnsRequest getApplyPreviewColumns();

    public native AsOfJoinTablesRequest getAsOfJoin();

    public native ComboAggregateRequest getComboAggregate();

    public native CreateInputTableRequest getCreateInputTable();

    public native CrossJoinTablesRequest getCrossJoin();

    public native DropColumnsRequest getDropColumns();

    public native EmptyTableRequest getEmptyTable();

    public native ExactJoinTablesRequest getExactJoin();

    public native FetchPandasTableRequest getFetchPandasTable();

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

    public native SortTableRequest getSort();

    public native HeadOrTailRequest getTail();

    public native HeadOrTailByRequest getTailBy();

    public native TimeTableRequest getTimeTable();

    public native UngroupRequest getUngroup();

    public native UnstructuredFilterTableRequest getUnstructuredFilter();

    public native SelectOrUpdateRequest getUpdate();

    public native SelectOrUpdateRequest getUpdateView();

    public native SelectOrUpdateRequest getView();

    public native boolean hasApplyPreviewColumns();

    public native boolean hasAsOfJoin();

    public native boolean hasComboAggregate();

    public native boolean hasCreateInputTable();

    public native boolean hasCrossJoin();

    public native boolean hasDropColumns();

    public native boolean hasEmptyTable();

    public native boolean hasExactJoin();

    public native boolean hasFetchPandasTable();

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

    public native boolean hasSort();

    public native boolean hasTail();

    public native boolean hasTailBy();

    public native boolean hasTimeTable();

    public native boolean hasUngroup();

    public native boolean hasUnstructuredFilter();

    public native boolean hasUpdate();

    public native boolean hasUpdateView();

    public native boolean hasView();

    public native Uint8Array serializeBinary();

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

    public native void setFetchPandasTable();

    public native void setFetchPandasTable(FetchPandasTableRequest value);

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

    public native void setUpdateView();

    public native void setUpdateView(SelectOrUpdateRequest value);

    public native void setView();

    public native void setView(SelectOrUpdateRequest value);

    public native Operation.ToObjectReturnType0 toObject();

    public native Operation.ToObjectReturnType0 toObject(boolean includeInstance);
}
