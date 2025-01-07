//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.hierarchicaltable_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.Condition;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.SortDescriptor;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.ticket_pb.Ticket;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.hierarchicaltable_pb.HierarchicalTableApplyRequest",
        namespace = JsPackage.GLOBAL)
public class HierarchicalTableApplyRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface FiltersListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface AndFieldType {
                @JsOverlay
                static HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType.AndFieldType create() {
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
                        static HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType.CompareFieldType.LhsFieldType.LiteralFieldType create() {
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
                        static HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType.CompareFieldType.LhsFieldType.ReferenceFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        String getColumnName();

                        @JsProperty
                        void setColumnName(String columnName);
                    }

                    @JsOverlay
                    static HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType.CompareFieldType.LhsFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType.CompareFieldType.LhsFieldType.LiteralFieldType getLiteral();

                    @JsProperty
                    HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType.CompareFieldType.LhsFieldType.ReferenceFieldType getReference();

                    @JsProperty
                    void setLiteral(
                            HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType.CompareFieldType.LhsFieldType.LiteralFieldType literal);

                    @JsProperty
                    void setReference(
                            HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType.CompareFieldType.LhsFieldType.ReferenceFieldType reference);
                }

                @JsOverlay
                static HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType.CompareFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                double getCaseSensitivity();

                @JsProperty
                HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType.CompareFieldType.LhsFieldType getLhs();

                @JsProperty
                double getOperation();

                @JsProperty
                Object getRhs();

                @JsProperty
                void setCaseSensitivity(double caseSensitivity);

                @JsProperty
                void setLhs(
                        HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType.CompareFieldType.LhsFieldType lhs);

                @JsProperty
                void setOperation(double operation);

                @JsProperty
                void setRhs(Object rhs);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ContainsFieldType {
                @JsOverlay
                static HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType.ContainsFieldType create() {
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
                static HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType.InvokeFieldType create() {
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
                static HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType.IsNullFieldType create() {
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
                static HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType.MatchesFieldType create() {
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
                static HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType.NotFieldType create() {
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
                static HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType.OrFieldType create() {
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
                static HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType.Pb_inFieldType create() {
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
                static HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType.SearchFieldType create() {
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
            static HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType.AndFieldType getAnd();

            @JsProperty
            HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType.CompareFieldType getCompare();

            @JsProperty
            HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType.ContainsFieldType getContains();

            @JsProperty
            HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType.InvokeFieldType getInvoke();

            @JsProperty
            HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType.IsNullFieldType getIsNull();

            @JsProperty
            HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType.MatchesFieldType getMatches();

            @JsProperty
            HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType.NotFieldType getNot();

            @JsProperty
            HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType.OrFieldType getOr();

            @JsProperty
            HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType.Pb_inFieldType getPb_in();

            @JsProperty
            HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType.SearchFieldType getSearch();

            @JsProperty
            void setAnd(
                    HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType.AndFieldType and);

            @JsProperty
            void setCompare(
                    HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType.CompareFieldType compare);

            @JsProperty
            void setContains(
                    HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType.ContainsFieldType contains);

            @JsProperty
            void setInvoke(
                    HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType.InvokeFieldType invoke);

            @JsProperty
            void setIsNull(
                    HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType.IsNullFieldType isNull);

            @JsProperty
            void setMatches(
                    HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType.MatchesFieldType matches);

            @JsProperty
            void setNot(
                    HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType.NotFieldType not);

            @JsProperty
            void setOr(
                    HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType.OrFieldType or);

            @JsProperty
            void setPb_in(
                    HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType.Pb_inFieldType pb_in);

            @JsProperty
            void setSearch(
                    HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType.SearchFieldType search);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultHierarchicalTableIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static HierarchicalTableApplyRequest.ToObjectReturnType.ResultHierarchicalTableIdFieldType.GetTicketUnionType of(
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
            static HierarchicalTableApplyRequest.ToObjectReturnType.ResultHierarchicalTableIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            HierarchicalTableApplyRequest.ToObjectReturnType.ResultHierarchicalTableIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    HierarchicalTableApplyRequest.ToObjectReturnType.ResultHierarchicalTableIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<HierarchicalTableApplyRequest.ToObjectReturnType.ResultHierarchicalTableIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<HierarchicalTableApplyRequest.ToObjectReturnType.ResultHierarchicalTableIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SortsListFieldType {
            @JsOverlay
            static HierarchicalTableApplyRequest.ToObjectReturnType.SortsListFieldType create() {
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
        static HierarchicalTableApplyRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType> getFiltersList();

        @JsProperty
        Object getInputHierarchicalTableId();

        @JsProperty
        HierarchicalTableApplyRequest.ToObjectReturnType.ResultHierarchicalTableIdFieldType getResultHierarchicalTableId();

        @JsProperty
        JsArray<HierarchicalTableApplyRequest.ToObjectReturnType.SortsListFieldType> getSortsList();

        @JsOverlay
        default void setFiltersList(
                HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType[] filtersList) {
            setFiltersList(
                    Js.<JsArray<HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType>>uncheckedCast(
                            filtersList));
        }

        @JsProperty
        void setFiltersList(
                JsArray<HierarchicalTableApplyRequest.ToObjectReturnType.FiltersListFieldType> filtersList);

        @JsProperty
        void setInputHierarchicalTableId(Object inputHierarchicalTableId);

        @JsProperty
        void setResultHierarchicalTableId(
                HierarchicalTableApplyRequest.ToObjectReturnType.ResultHierarchicalTableIdFieldType resultHierarchicalTableId);

        @JsProperty
        void setSortsList(
                JsArray<HierarchicalTableApplyRequest.ToObjectReturnType.SortsListFieldType> sortsList);

        @JsOverlay
        default void setSortsList(
                HierarchicalTableApplyRequest.ToObjectReturnType.SortsListFieldType[] sortsList) {
            setSortsList(
                    Js.<JsArray<HierarchicalTableApplyRequest.ToObjectReturnType.SortsListFieldType>>uncheckedCast(
                            sortsList));
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface FiltersListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface AndFieldType {
                @JsOverlay
                static HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType.AndFieldType create() {
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
                        static HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType.CompareFieldType.LhsFieldType.LiteralFieldType create() {
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
                        static HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType.CompareFieldType.LhsFieldType.ReferenceFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        String getColumnName();

                        @JsProperty
                        void setColumnName(String columnName);
                    }

                    @JsOverlay
                    static HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType.CompareFieldType.LhsFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType.CompareFieldType.LhsFieldType.LiteralFieldType getLiteral();

                    @JsProperty
                    HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType.CompareFieldType.LhsFieldType.ReferenceFieldType getReference();

                    @JsProperty
                    void setLiteral(
                            HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType.CompareFieldType.LhsFieldType.LiteralFieldType literal);

                    @JsProperty
                    void setReference(
                            HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType.CompareFieldType.LhsFieldType.ReferenceFieldType reference);
                }

                @JsOverlay
                static HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType.CompareFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                double getCaseSensitivity();

                @JsProperty
                HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType.CompareFieldType.LhsFieldType getLhs();

                @JsProperty
                double getOperation();

                @JsProperty
                Object getRhs();

                @JsProperty
                void setCaseSensitivity(double caseSensitivity);

                @JsProperty
                void setLhs(
                        HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType.CompareFieldType.LhsFieldType lhs);

                @JsProperty
                void setOperation(double operation);

                @JsProperty
                void setRhs(Object rhs);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ContainsFieldType {
                @JsOverlay
                static HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType.ContainsFieldType create() {
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
                static HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType.InvokeFieldType create() {
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
                static HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType.IsNullFieldType create() {
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
                static HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType.MatchesFieldType create() {
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
                static HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType.NotFieldType create() {
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
                static HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType.OrFieldType create() {
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
                static HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType.Pb_inFieldType create() {
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
                static HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType.SearchFieldType create() {
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
            static HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType.AndFieldType getAnd();

            @JsProperty
            HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType.CompareFieldType getCompare();

            @JsProperty
            HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType.ContainsFieldType getContains();

            @JsProperty
            HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType.InvokeFieldType getInvoke();

            @JsProperty
            HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType.IsNullFieldType getIsNull();

            @JsProperty
            HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType.MatchesFieldType getMatches();

            @JsProperty
            HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType.NotFieldType getNot();

            @JsProperty
            HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType.OrFieldType getOr();

            @JsProperty
            HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType.Pb_inFieldType getPb_in();

            @JsProperty
            HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType.SearchFieldType getSearch();

            @JsProperty
            void setAnd(
                    HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType.AndFieldType and);

            @JsProperty
            void setCompare(
                    HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType.CompareFieldType compare);

            @JsProperty
            void setContains(
                    HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType.ContainsFieldType contains);

            @JsProperty
            void setInvoke(
                    HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType.InvokeFieldType invoke);

            @JsProperty
            void setIsNull(
                    HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType.IsNullFieldType isNull);

            @JsProperty
            void setMatches(
                    HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType.MatchesFieldType matches);

            @JsProperty
            void setNot(
                    HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType.NotFieldType not);

            @JsProperty
            void setOr(
                    HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType.OrFieldType or);

            @JsProperty
            void setPb_in(
                    HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType.Pb_inFieldType pb_in);

            @JsProperty
            void setSearch(
                    HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType.SearchFieldType search);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultHierarchicalTableIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static HierarchicalTableApplyRequest.ToObjectReturnType0.ResultHierarchicalTableIdFieldType.GetTicketUnionType of(
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
            static HierarchicalTableApplyRequest.ToObjectReturnType0.ResultHierarchicalTableIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            HierarchicalTableApplyRequest.ToObjectReturnType0.ResultHierarchicalTableIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    HierarchicalTableApplyRequest.ToObjectReturnType0.ResultHierarchicalTableIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<HierarchicalTableApplyRequest.ToObjectReturnType0.ResultHierarchicalTableIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<HierarchicalTableApplyRequest.ToObjectReturnType0.ResultHierarchicalTableIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SortsListFieldType {
            @JsOverlay
            static HierarchicalTableApplyRequest.ToObjectReturnType0.SortsListFieldType create() {
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
        static HierarchicalTableApplyRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType> getFiltersList();

        @JsProperty
        Object getInputHierarchicalTableId();

        @JsProperty
        HierarchicalTableApplyRequest.ToObjectReturnType0.ResultHierarchicalTableIdFieldType getResultHierarchicalTableId();

        @JsProperty
        JsArray<HierarchicalTableApplyRequest.ToObjectReturnType0.SortsListFieldType> getSortsList();

        @JsOverlay
        default void setFiltersList(
                HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType[] filtersList) {
            setFiltersList(
                    Js.<JsArray<HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType>>uncheckedCast(
                            filtersList));
        }

        @JsProperty
        void setFiltersList(
                JsArray<HierarchicalTableApplyRequest.ToObjectReturnType0.FiltersListFieldType> filtersList);

        @JsProperty
        void setInputHierarchicalTableId(Object inputHierarchicalTableId);

        @JsProperty
        void setResultHierarchicalTableId(
                HierarchicalTableApplyRequest.ToObjectReturnType0.ResultHierarchicalTableIdFieldType resultHierarchicalTableId);

        @JsProperty
        void setSortsList(
                JsArray<HierarchicalTableApplyRequest.ToObjectReturnType0.SortsListFieldType> sortsList);

        @JsOverlay
        default void setSortsList(
                HierarchicalTableApplyRequest.ToObjectReturnType0.SortsListFieldType[] sortsList) {
            setSortsList(
                    Js.<JsArray<HierarchicalTableApplyRequest.ToObjectReturnType0.SortsListFieldType>>uncheckedCast(
                            sortsList));
        }
    }

    public static native HierarchicalTableApplyRequest deserializeBinary(Uint8Array bytes);

    public static native HierarchicalTableApplyRequest deserializeBinaryFromReader(
            HierarchicalTableApplyRequest message, Object reader);

    public static native void serializeBinaryToWriter(
            HierarchicalTableApplyRequest message, Object writer);

    public static native HierarchicalTableApplyRequest.ToObjectReturnType toObject(
            boolean includeInstance, HierarchicalTableApplyRequest msg);

    public native Condition addFilters();

    public native Condition addFilters(Condition value, double index);

    public native Condition addFilters(Condition value);

    public native SortDescriptor addSorts();

    public native SortDescriptor addSorts(SortDescriptor value, double index);

    public native SortDescriptor addSorts(SortDescriptor value);

    public native void clearFiltersList();

    public native void clearInputHierarchicalTableId();

    public native void clearResultHierarchicalTableId();

    public native void clearSortsList();

    public native JsArray<Condition> getFiltersList();

    public native Ticket getInputHierarchicalTableId();

    public native Ticket getResultHierarchicalTableId();

    public native JsArray<SortDescriptor> getSortsList();

    public native boolean hasInputHierarchicalTableId();

    public native boolean hasResultHierarchicalTableId();

    public native Uint8Array serializeBinary();

    @JsOverlay
    public final void setFiltersList(Condition[] value) {
        setFiltersList(Js.<JsArray<Condition>>uncheckedCast(value));
    }

    public native void setFiltersList(JsArray<Condition> value);

    public native void setInputHierarchicalTableId();

    public native void setInputHierarchicalTableId(Ticket value);

    public native void setResultHierarchicalTableId();

    public native void setResultHierarchicalTableId(Ticket value);

    public native void setSortsList(JsArray<SortDescriptor> value);

    @JsOverlay
    public final void setSortsList(SortDescriptor[] value) {
        setSortsList(Js.<JsArray<SortDescriptor>>uncheckedCast(value));
    }

    public native HierarchicalTableApplyRequest.ToObjectReturnType0 toObject();

    public native HierarchicalTableApplyRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
