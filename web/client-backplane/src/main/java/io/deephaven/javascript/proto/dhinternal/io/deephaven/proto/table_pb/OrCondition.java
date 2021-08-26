package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.table_pb.OrCondition",
    namespace = JsPackage.GLOBAL)
public class OrCondition {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface FiltersListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface AndFieldType {
                @JsOverlay
                static OrCondition.ToObjectReturnType.FiltersListFieldType.AndFieldType create() {
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
                        static OrCondition.ToObjectReturnType.FiltersListFieldType.CompareFieldType.LhsFieldType.LiteralFieldType create() {
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
                        static OrCondition.ToObjectReturnType.FiltersListFieldType.CompareFieldType.LhsFieldType.ReferenceFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        String getColumnName();

                        @JsProperty
                        void setColumnName(String columnName);
                    }

                    @JsOverlay
                    static OrCondition.ToObjectReturnType.FiltersListFieldType.CompareFieldType.LhsFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    OrCondition.ToObjectReturnType.FiltersListFieldType.CompareFieldType.LhsFieldType.LiteralFieldType getLiteral();

                    @JsProperty
                    OrCondition.ToObjectReturnType.FiltersListFieldType.CompareFieldType.LhsFieldType.ReferenceFieldType getReference();

                    @JsProperty
                    void setLiteral(
                        OrCondition.ToObjectReturnType.FiltersListFieldType.CompareFieldType.LhsFieldType.LiteralFieldType literal);

                    @JsProperty
                    void setReference(
                        OrCondition.ToObjectReturnType.FiltersListFieldType.CompareFieldType.LhsFieldType.ReferenceFieldType reference);
                }

                @JsOverlay
                static OrCondition.ToObjectReturnType.FiltersListFieldType.CompareFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                double getCaseSensitivity();

                @JsProperty
                OrCondition.ToObjectReturnType.FiltersListFieldType.CompareFieldType.LhsFieldType getLhs();

                @JsProperty
                double getOperation();

                @JsProperty
                Object getRhs();

                @JsProperty
                void setCaseSensitivity(double caseSensitivity);

                @JsProperty
                void setLhs(
                    OrCondition.ToObjectReturnType.FiltersListFieldType.CompareFieldType.LhsFieldType lhs);

                @JsProperty
                void setOperation(double operation);

                @JsProperty
                void setRhs(Object rhs);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ContainsFieldType {
                @JsOverlay
                static OrCondition.ToObjectReturnType.FiltersListFieldType.ContainsFieldType create() {
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
                static OrCondition.ToObjectReturnType.FiltersListFieldType.InvokeFieldType create() {
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
                static OrCondition.ToObjectReturnType.FiltersListFieldType.IsNullFieldType create() {
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
                static OrCondition.ToObjectReturnType.FiltersListFieldType.MatchesFieldType create() {
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
                static OrCondition.ToObjectReturnType.FiltersListFieldType.NotFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getFilter();

                @JsProperty
                void setFilter(Object filter);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface Pb_inFieldType {
                @JsOverlay
                static OrCondition.ToObjectReturnType.FiltersListFieldType.Pb_inFieldType create() {
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
                static OrCondition.ToObjectReturnType.FiltersListFieldType.SearchFieldType create() {
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
            static OrCondition.ToObjectReturnType.FiltersListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            OrCondition.ToObjectReturnType.FiltersListFieldType.AndFieldType getAnd();

            @JsProperty
            OrCondition.ToObjectReturnType.FiltersListFieldType.CompareFieldType getCompare();

            @JsProperty
            OrCondition.ToObjectReturnType.FiltersListFieldType.ContainsFieldType getContains();

            @JsProperty
            OrCondition.ToObjectReturnType.FiltersListFieldType.InvokeFieldType getInvoke();

            @JsProperty
            OrCondition.ToObjectReturnType.FiltersListFieldType.IsNullFieldType getIsNull();

            @JsProperty
            OrCondition.ToObjectReturnType.FiltersListFieldType.MatchesFieldType getMatches();

            @JsProperty
            OrCondition.ToObjectReturnType.FiltersListFieldType.NotFieldType getNot();

            @JsProperty
            Object getOr();

            @JsProperty
            OrCondition.ToObjectReturnType.FiltersListFieldType.Pb_inFieldType getPb_in();

            @JsProperty
            OrCondition.ToObjectReturnType.FiltersListFieldType.SearchFieldType getSearch();

            @JsProperty
            void setAnd(OrCondition.ToObjectReturnType.FiltersListFieldType.AndFieldType and);

            @JsProperty
            void setCompare(
                OrCondition.ToObjectReturnType.FiltersListFieldType.CompareFieldType compare);

            @JsProperty
            void setContains(
                OrCondition.ToObjectReturnType.FiltersListFieldType.ContainsFieldType contains);

            @JsProperty
            void setInvoke(
                OrCondition.ToObjectReturnType.FiltersListFieldType.InvokeFieldType invoke);

            @JsProperty
            void setIsNull(
                OrCondition.ToObjectReturnType.FiltersListFieldType.IsNullFieldType isNull);

            @JsProperty
            void setMatches(
                OrCondition.ToObjectReturnType.FiltersListFieldType.MatchesFieldType matches);

            @JsProperty
            void setNot(OrCondition.ToObjectReturnType.FiltersListFieldType.NotFieldType not);

            @JsProperty
            void setOr(Object or);

            @JsProperty
            void setPb_in(OrCondition.ToObjectReturnType.FiltersListFieldType.Pb_inFieldType pb_in);

            @JsProperty
            void setSearch(
                OrCondition.ToObjectReturnType.FiltersListFieldType.SearchFieldType search);
        }

        @JsOverlay
        static OrCondition.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<OrCondition.ToObjectReturnType.FiltersListFieldType> getFiltersList();

        @JsOverlay
        default void setFiltersList(
            OrCondition.ToObjectReturnType.FiltersListFieldType[] filtersList) {
            setFiltersList(
                Js.<JsArray<OrCondition.ToObjectReturnType.FiltersListFieldType>>uncheckedCast(
                    filtersList));
        }

        @JsProperty
        void setFiltersList(
            JsArray<OrCondition.ToObjectReturnType.FiltersListFieldType> filtersList);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface FiltersListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface AndFieldType {
                @JsOverlay
                static OrCondition.ToObjectReturnType0.FiltersListFieldType.AndFieldType create() {
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
                        static OrCondition.ToObjectReturnType0.FiltersListFieldType.CompareFieldType.LhsFieldType.LiteralFieldType create() {
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
                        static OrCondition.ToObjectReturnType0.FiltersListFieldType.CompareFieldType.LhsFieldType.ReferenceFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        String getColumnName();

                        @JsProperty
                        void setColumnName(String columnName);
                    }

                    @JsOverlay
                    static OrCondition.ToObjectReturnType0.FiltersListFieldType.CompareFieldType.LhsFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    OrCondition.ToObjectReturnType0.FiltersListFieldType.CompareFieldType.LhsFieldType.LiteralFieldType getLiteral();

                    @JsProperty
                    OrCondition.ToObjectReturnType0.FiltersListFieldType.CompareFieldType.LhsFieldType.ReferenceFieldType getReference();

                    @JsProperty
                    void setLiteral(
                        OrCondition.ToObjectReturnType0.FiltersListFieldType.CompareFieldType.LhsFieldType.LiteralFieldType literal);

                    @JsProperty
                    void setReference(
                        OrCondition.ToObjectReturnType0.FiltersListFieldType.CompareFieldType.LhsFieldType.ReferenceFieldType reference);
                }

                @JsOverlay
                static OrCondition.ToObjectReturnType0.FiltersListFieldType.CompareFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                double getCaseSensitivity();

                @JsProperty
                OrCondition.ToObjectReturnType0.FiltersListFieldType.CompareFieldType.LhsFieldType getLhs();

                @JsProperty
                double getOperation();

                @JsProperty
                Object getRhs();

                @JsProperty
                void setCaseSensitivity(double caseSensitivity);

                @JsProperty
                void setLhs(
                    OrCondition.ToObjectReturnType0.FiltersListFieldType.CompareFieldType.LhsFieldType lhs);

                @JsProperty
                void setOperation(double operation);

                @JsProperty
                void setRhs(Object rhs);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ContainsFieldType {
                @JsOverlay
                static OrCondition.ToObjectReturnType0.FiltersListFieldType.ContainsFieldType create() {
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
                static OrCondition.ToObjectReturnType0.FiltersListFieldType.InvokeFieldType create() {
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
                static OrCondition.ToObjectReturnType0.FiltersListFieldType.IsNullFieldType create() {
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
                static OrCondition.ToObjectReturnType0.FiltersListFieldType.MatchesFieldType create() {
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
                static OrCondition.ToObjectReturnType0.FiltersListFieldType.NotFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Object getFilter();

                @JsProperty
                void setFilter(Object filter);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface Pb_inFieldType {
                @JsOverlay
                static OrCondition.ToObjectReturnType0.FiltersListFieldType.Pb_inFieldType create() {
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
                static OrCondition.ToObjectReturnType0.FiltersListFieldType.SearchFieldType create() {
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
            static OrCondition.ToObjectReturnType0.FiltersListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            OrCondition.ToObjectReturnType0.FiltersListFieldType.AndFieldType getAnd();

            @JsProperty
            OrCondition.ToObjectReturnType0.FiltersListFieldType.CompareFieldType getCompare();

            @JsProperty
            OrCondition.ToObjectReturnType0.FiltersListFieldType.ContainsFieldType getContains();

            @JsProperty
            OrCondition.ToObjectReturnType0.FiltersListFieldType.InvokeFieldType getInvoke();

            @JsProperty
            OrCondition.ToObjectReturnType0.FiltersListFieldType.IsNullFieldType getIsNull();

            @JsProperty
            OrCondition.ToObjectReturnType0.FiltersListFieldType.MatchesFieldType getMatches();

            @JsProperty
            OrCondition.ToObjectReturnType0.FiltersListFieldType.NotFieldType getNot();

            @JsProperty
            Object getOr();

            @JsProperty
            OrCondition.ToObjectReturnType0.FiltersListFieldType.Pb_inFieldType getPb_in();

            @JsProperty
            OrCondition.ToObjectReturnType0.FiltersListFieldType.SearchFieldType getSearch();

            @JsProperty
            void setAnd(OrCondition.ToObjectReturnType0.FiltersListFieldType.AndFieldType and);

            @JsProperty
            void setCompare(
                OrCondition.ToObjectReturnType0.FiltersListFieldType.CompareFieldType compare);

            @JsProperty
            void setContains(
                OrCondition.ToObjectReturnType0.FiltersListFieldType.ContainsFieldType contains);

            @JsProperty
            void setInvoke(
                OrCondition.ToObjectReturnType0.FiltersListFieldType.InvokeFieldType invoke);

            @JsProperty
            void setIsNull(
                OrCondition.ToObjectReturnType0.FiltersListFieldType.IsNullFieldType isNull);

            @JsProperty
            void setMatches(
                OrCondition.ToObjectReturnType0.FiltersListFieldType.MatchesFieldType matches);

            @JsProperty
            void setNot(OrCondition.ToObjectReturnType0.FiltersListFieldType.NotFieldType not);

            @JsProperty
            void setOr(Object or);

            @JsProperty
            void setPb_in(
                OrCondition.ToObjectReturnType0.FiltersListFieldType.Pb_inFieldType pb_in);

            @JsProperty
            void setSearch(
                OrCondition.ToObjectReturnType0.FiltersListFieldType.SearchFieldType search);
        }

        @JsOverlay
        static OrCondition.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<OrCondition.ToObjectReturnType0.FiltersListFieldType> getFiltersList();

        @JsOverlay
        default void setFiltersList(
            OrCondition.ToObjectReturnType0.FiltersListFieldType[] filtersList) {
            setFiltersList(
                Js.<JsArray<OrCondition.ToObjectReturnType0.FiltersListFieldType>>uncheckedCast(
                    filtersList));
        }

        @JsProperty
        void setFiltersList(
            JsArray<OrCondition.ToObjectReturnType0.FiltersListFieldType> filtersList);
    }

    public static native OrCondition deserializeBinary(Uint8Array bytes);

    public static native OrCondition deserializeBinaryFromReader(OrCondition message,
        Object reader);

    public static native void serializeBinaryToWriter(OrCondition message, Object writer);

    public static native OrCondition.ToObjectReturnType toObject(
        boolean includeInstance, OrCondition msg);

    public native Condition addFilters();

    public native Condition addFilters(Condition value, double index);

    public native Condition addFilters(Condition value);

    public native void clearFiltersList();

    public native JsArray<Condition> getFiltersList();

    public native Uint8Array serializeBinary();

    @JsOverlay
    public final void setFiltersList(Condition[] value) {
        setFiltersList(Js.<JsArray<Condition>>uncheckedCast(value));
    }

    public native void setFiltersList(JsArray<Condition> value);

    public native OrCondition.ToObjectReturnType0 toObject();

    public native OrCondition.ToObjectReturnType0 toObject(boolean includeInstance);
}
