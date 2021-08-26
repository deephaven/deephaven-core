package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.condition.DataCase;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.Condition",
        namespace = JsPackage.GLOBAL)
public class Condition {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface AndFieldType {
            @JsOverlay
            static Condition.ToObjectReturnType.AndFieldType create() {
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
                    static Condition.ToObjectReturnType.CompareFieldType.LhsFieldType.LiteralFieldType create() {
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
                    static Condition.ToObjectReturnType.CompareFieldType.LhsFieldType.ReferenceFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    String getColumnName();

                    @JsProperty
                    void setColumnName(String columnName);
                }

                @JsOverlay
                static Condition.ToObjectReturnType.CompareFieldType.LhsFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Condition.ToObjectReturnType.CompareFieldType.LhsFieldType.LiteralFieldType getLiteral();

                @JsProperty
                Condition.ToObjectReturnType.CompareFieldType.LhsFieldType.ReferenceFieldType getReference();

                @JsProperty
                void setLiteral(
                        Condition.ToObjectReturnType.CompareFieldType.LhsFieldType.LiteralFieldType literal);

                @JsProperty
                void setReference(
                        Condition.ToObjectReturnType.CompareFieldType.LhsFieldType.ReferenceFieldType reference);
            }

            @JsOverlay
            static Condition.ToObjectReturnType.CompareFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getCaseSensitivity();

            @JsProperty
            Condition.ToObjectReturnType.CompareFieldType.LhsFieldType getLhs();

            @JsProperty
            double getOperation();

            @JsProperty
            Object getRhs();

            @JsProperty
            void setCaseSensitivity(double caseSensitivity);

            @JsProperty
            void setLhs(Condition.ToObjectReturnType.CompareFieldType.LhsFieldType lhs);

            @JsProperty
            void setOperation(double operation);

            @JsProperty
            void setRhs(Object rhs);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ContainsFieldType {
            @JsOverlay
            static Condition.ToObjectReturnType.ContainsFieldType create() {
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
            static Condition.ToObjectReturnType.InvokeFieldType create() {
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
            static Condition.ToObjectReturnType.IsNullFieldType create() {
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
            static Condition.ToObjectReturnType.MatchesFieldType create() {
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
            static Condition.ToObjectReturnType.NotFieldType create() {
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
            static Condition.ToObjectReturnType.OrFieldType create() {
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
            static Condition.ToObjectReturnType.Pb_inFieldType create() {
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
            static Condition.ToObjectReturnType.SearchFieldType create() {
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
        static Condition.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Condition.ToObjectReturnType.AndFieldType getAnd();

        @JsProperty
        Condition.ToObjectReturnType.CompareFieldType getCompare();

        @JsProperty
        Condition.ToObjectReturnType.ContainsFieldType getContains();

        @JsProperty
        Condition.ToObjectReturnType.InvokeFieldType getInvoke();

        @JsProperty
        Condition.ToObjectReturnType.IsNullFieldType getIsNull();

        @JsProperty
        Condition.ToObjectReturnType.MatchesFieldType getMatches();

        @JsProperty
        Condition.ToObjectReturnType.NotFieldType getNot();

        @JsProperty
        Condition.ToObjectReturnType.OrFieldType getOr();

        @JsProperty
        Condition.ToObjectReturnType.Pb_inFieldType getPb_in();

        @JsProperty
        Condition.ToObjectReturnType.SearchFieldType getSearch();

        @JsProperty
        void setAnd(Condition.ToObjectReturnType.AndFieldType and);

        @JsProperty
        void setCompare(Condition.ToObjectReturnType.CompareFieldType compare);

        @JsProperty
        void setContains(Condition.ToObjectReturnType.ContainsFieldType contains);

        @JsProperty
        void setInvoke(Condition.ToObjectReturnType.InvokeFieldType invoke);

        @JsProperty
        void setIsNull(Condition.ToObjectReturnType.IsNullFieldType isNull);

        @JsProperty
        void setMatches(Condition.ToObjectReturnType.MatchesFieldType matches);

        @JsProperty
        void setNot(Condition.ToObjectReturnType.NotFieldType not);

        @JsProperty
        void setOr(Condition.ToObjectReturnType.OrFieldType or);

        @JsProperty
        void setPb_in(Condition.ToObjectReturnType.Pb_inFieldType pb_in);

        @JsProperty
        void setSearch(Condition.ToObjectReturnType.SearchFieldType search);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface AndFieldType {
            @JsOverlay
            static Condition.ToObjectReturnType0.AndFieldType create() {
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
                    static Condition.ToObjectReturnType0.CompareFieldType.LhsFieldType.LiteralFieldType create() {
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
                    static Condition.ToObjectReturnType0.CompareFieldType.LhsFieldType.ReferenceFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    String getColumnName();

                    @JsProperty
                    void setColumnName(String columnName);
                }

                @JsOverlay
                static Condition.ToObjectReturnType0.CompareFieldType.LhsFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                Condition.ToObjectReturnType0.CompareFieldType.LhsFieldType.LiteralFieldType getLiteral();

                @JsProperty
                Condition.ToObjectReturnType0.CompareFieldType.LhsFieldType.ReferenceFieldType getReference();

                @JsProperty
                void setLiteral(
                        Condition.ToObjectReturnType0.CompareFieldType.LhsFieldType.LiteralFieldType literal);

                @JsProperty
                void setReference(
                        Condition.ToObjectReturnType0.CompareFieldType.LhsFieldType.ReferenceFieldType reference);
            }

            @JsOverlay
            static Condition.ToObjectReturnType0.CompareFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getCaseSensitivity();

            @JsProperty
            Condition.ToObjectReturnType0.CompareFieldType.LhsFieldType getLhs();

            @JsProperty
            double getOperation();

            @JsProperty
            Object getRhs();

            @JsProperty
            void setCaseSensitivity(double caseSensitivity);

            @JsProperty
            void setLhs(Condition.ToObjectReturnType0.CompareFieldType.LhsFieldType lhs);

            @JsProperty
            void setOperation(double operation);

            @JsProperty
            void setRhs(Object rhs);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ContainsFieldType {
            @JsOverlay
            static Condition.ToObjectReturnType0.ContainsFieldType create() {
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
            static Condition.ToObjectReturnType0.InvokeFieldType create() {
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
            static Condition.ToObjectReturnType0.IsNullFieldType create() {
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
            static Condition.ToObjectReturnType0.MatchesFieldType create() {
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
            static Condition.ToObjectReturnType0.NotFieldType create() {
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
            static Condition.ToObjectReturnType0.OrFieldType create() {
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
            static Condition.ToObjectReturnType0.Pb_inFieldType create() {
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
            static Condition.ToObjectReturnType0.SearchFieldType create() {
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
        static Condition.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Condition.ToObjectReturnType0.AndFieldType getAnd();

        @JsProperty
        Condition.ToObjectReturnType0.CompareFieldType getCompare();

        @JsProperty
        Condition.ToObjectReturnType0.ContainsFieldType getContains();

        @JsProperty
        Condition.ToObjectReturnType0.InvokeFieldType getInvoke();

        @JsProperty
        Condition.ToObjectReturnType0.IsNullFieldType getIsNull();

        @JsProperty
        Condition.ToObjectReturnType0.MatchesFieldType getMatches();

        @JsProperty
        Condition.ToObjectReturnType0.NotFieldType getNot();

        @JsProperty
        Condition.ToObjectReturnType0.OrFieldType getOr();

        @JsProperty
        Condition.ToObjectReturnType0.Pb_inFieldType getPb_in();

        @JsProperty
        Condition.ToObjectReturnType0.SearchFieldType getSearch();

        @JsProperty
        void setAnd(Condition.ToObjectReturnType0.AndFieldType and);

        @JsProperty
        void setCompare(Condition.ToObjectReturnType0.CompareFieldType compare);

        @JsProperty
        void setContains(Condition.ToObjectReturnType0.ContainsFieldType contains);

        @JsProperty
        void setInvoke(Condition.ToObjectReturnType0.InvokeFieldType invoke);

        @JsProperty
        void setIsNull(Condition.ToObjectReturnType0.IsNullFieldType isNull);

        @JsProperty
        void setMatches(Condition.ToObjectReturnType0.MatchesFieldType matches);

        @JsProperty
        void setNot(Condition.ToObjectReturnType0.NotFieldType not);

        @JsProperty
        void setOr(Condition.ToObjectReturnType0.OrFieldType or);

        @JsProperty
        void setPb_in(Condition.ToObjectReturnType0.Pb_inFieldType pb_in);

        @JsProperty
        void setSearch(Condition.ToObjectReturnType0.SearchFieldType search);
    }

    public static native Condition deserializeBinary(Uint8Array bytes);

    public static native Condition deserializeBinaryFromReader(Condition message, Object reader);

    public static native void serializeBinaryToWriter(Condition message, Object writer);

    public static native Condition.ToObjectReturnType toObject(
            boolean includeInstance, Condition msg);

    public native void clearAnd();

    public native void clearCompare();

    public native void clearContains();

    public native void clearIn();

    public native void clearInvoke();

    public native void clearIsNull();

    public native void clearMatches();

    public native void clearNot();

    public native void clearOr();

    public native void clearSearch();

    public native AndCondition getAnd();

    public native CompareCondition getCompare();

    public native ContainsCondition getContains();

    public native DataCase getDataCase();

    public native InCondition getIn();

    public native InvokeCondition getInvoke();

    public native IsNullCondition getIsNull();

    public native MatchesCondition getMatches();

    public native NotCondition getNot();

    public native OrCondition getOr();

    public native SearchCondition getSearch();

    public native boolean hasAnd();

    public native boolean hasCompare();

    public native boolean hasContains();

    public native boolean hasIn();

    public native boolean hasInvoke();

    public native boolean hasIsNull();

    public native boolean hasMatches();

    public native boolean hasNot();

    public native boolean hasOr();

    public native boolean hasSearch();

    public native Uint8Array serializeBinary();

    public native void setAnd();

    public native void setAnd(AndCondition value);

    public native void setCompare();

    public native void setCompare(CompareCondition value);

    public native void setContains();

    public native void setContains(ContainsCondition value);

    public native void setIn();

    public native void setIn(InCondition value);

    public native void setInvoke();

    public native void setInvoke(InvokeCondition value);

    public native void setIsNull();

    public native void setIsNull(IsNullCondition value);

    public native void setMatches();

    public native void setMatches(MatchesCondition value);

    public native void setNot();

    public native void setNot(NotCondition value);

    public native void setOr();

    public native void setOr(OrCondition value);

    public native void setSearch();

    public native void setSearch(SearchCondition value);

    public native Condition.ToObjectReturnType0 toObject();

    public native Condition.ToObjectReturnType0 toObject(boolean includeInstance);
}
