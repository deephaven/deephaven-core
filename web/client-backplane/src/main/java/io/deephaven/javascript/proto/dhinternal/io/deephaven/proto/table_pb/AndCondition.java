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
    name = "dhinternal.io.deephaven.proto.table_pb.AndCondition",
    namespace = JsPackage.GLOBAL)
public class AndCondition {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface FiltersListFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface CompareFieldType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface LhsFieldType {
          @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
          public interface LiteralFieldType {
            @JsOverlay
            static AndCondition.ToObjectReturnType.FiltersListFieldType.CompareFieldType
                    .LhsFieldType.LiteralFieldType
                create() {
              return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getDoublevalue();

            @JsProperty
            double getLongvalue();

            @JsProperty
            double getNanotimevalue();

            @JsProperty
            String getStringvalue();

            @JsProperty
            boolean isBoolvalue();

            @JsProperty
            void setBoolvalue(boolean boolvalue);

            @JsProperty
            void setDoublevalue(double doublevalue);

            @JsProperty
            void setLongvalue(double longvalue);

            @JsProperty
            void setNanotimevalue(double nanotimevalue);

            @JsProperty
            void setStringvalue(String stringvalue);
          }

          @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
          public interface ReferenceFieldType {
            @JsOverlay
            static AndCondition.ToObjectReturnType.FiltersListFieldType.CompareFieldType
                    .LhsFieldType.ReferenceFieldType
                create() {
              return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getColumnname();

            @JsProperty
            void setColumnname(String columnname);
          }

          @JsOverlay
          static AndCondition.ToObjectReturnType.FiltersListFieldType.CompareFieldType.LhsFieldType
              create() {
            return Js.uncheckedCast(JsPropertyMap.of());
          }

          @JsProperty
          AndCondition.ToObjectReturnType.FiltersListFieldType.CompareFieldType.LhsFieldType
                  .LiteralFieldType
              getLiteral();

          @JsProperty
          AndCondition.ToObjectReturnType.FiltersListFieldType.CompareFieldType.LhsFieldType
                  .ReferenceFieldType
              getReference();

          @JsProperty
          void setLiteral(
              AndCondition.ToObjectReturnType.FiltersListFieldType.CompareFieldType.LhsFieldType
                      .LiteralFieldType
                  literal);

          @JsProperty
          void setReference(
              AndCondition.ToObjectReturnType.FiltersListFieldType.CompareFieldType.LhsFieldType
                      .ReferenceFieldType
                  reference);
        }

        @JsOverlay
        static AndCondition.ToObjectReturnType.FiltersListFieldType.CompareFieldType create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        AndCondition.ToObjectReturnType.FiltersListFieldType.CompareFieldType.LhsFieldType getLhs();

        @JsProperty
        double getOperation();

        @JsProperty
        Object getRhs();

        @JsProperty
        void setLhs(
            AndCondition.ToObjectReturnType.FiltersListFieldType.CompareFieldType.LhsFieldType lhs);

        @JsProperty
        void setOperation(double operation);

        @JsProperty
        void setRhs(Object rhs);
      }

      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface ContainsFieldType {
        @JsOverlay
        static AndCondition.ToObjectReturnType.FiltersListFieldType.ContainsFieldType create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getCasesensitivity();

        @JsProperty
        double getMatchtype();

        @JsProperty
        Object getReference();

        @JsProperty
        String getSearchstring();

        @JsProperty
        void setCasesensitivity(double casesensitivity);

        @JsProperty
        void setMatchtype(double matchtype);

        @JsProperty
        void setReference(Object reference);

        @JsProperty
        void setSearchstring(String searchstring);
      }

      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface InvokeFieldType {
        @JsOverlay
        static AndCondition.ToObjectReturnType.FiltersListFieldType.InvokeFieldType create() {
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
      public interface IsnullFieldType {
        @JsOverlay
        static AndCondition.ToObjectReturnType.FiltersListFieldType.IsnullFieldType create() {
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
        static AndCondition.ToObjectReturnType.FiltersListFieldType.MatchesFieldType create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getCasesensitivity();

        @JsProperty
        double getMatchtype();

        @JsProperty
        Object getReference();

        @JsProperty
        String getRegex();

        @JsProperty
        void setCasesensitivity(double casesensitivity);

        @JsProperty
        void setMatchtype(double matchtype);

        @JsProperty
        void setReference(Object reference);

        @JsProperty
        void setRegex(String regex);
      }

      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface NotFieldType {
        @JsOverlay
        static AndCondition.ToObjectReturnType.FiltersListFieldType.NotFieldType create() {
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
        static AndCondition.ToObjectReturnType.FiltersListFieldType.OrFieldType create() {
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
        static AndCondition.ToObjectReturnType.FiltersListFieldType.Pb_inFieldType create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<Object> getCandidatesList();

        @JsProperty
        double getCasesensitivity();

        @JsProperty
        double getMatchtype();

        @JsProperty
        Object getTarget();

        @JsProperty
        void setCandidatesList(JsArray<Object> candidatesList);

        @JsOverlay
        default void setCandidatesList(Object[] candidatesList) {
          setCandidatesList(Js.<JsArray<Object>>uncheckedCast(candidatesList));
        }

        @JsProperty
        void setCasesensitivity(double casesensitivity);

        @JsProperty
        void setMatchtype(double matchtype);

        @JsProperty
        void setTarget(Object target);
      }

      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface SearchFieldType {
        @JsOverlay
        static AndCondition.ToObjectReturnType.FiltersListFieldType.SearchFieldType create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<Object> getOptionalreferencesList();

        @JsProperty
        String getSearchstring();

        @JsProperty
        void setOptionalreferencesList(JsArray<Object> optionalreferencesList);

        @JsOverlay
        default void setOptionalreferencesList(Object[] optionalreferencesList) {
          setOptionalreferencesList(Js.<JsArray<Object>>uncheckedCast(optionalreferencesList));
        }

        @JsProperty
        void setSearchstring(String searchstring);
      }

      @JsOverlay
      static AndCondition.ToObjectReturnType.FiltersListFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      Object getAnd();

      @JsProperty
      AndCondition.ToObjectReturnType.FiltersListFieldType.CompareFieldType getCompare();

      @JsProperty
      AndCondition.ToObjectReturnType.FiltersListFieldType.ContainsFieldType getContains();

      @JsProperty
      AndCondition.ToObjectReturnType.FiltersListFieldType.InvokeFieldType getInvoke();

      @JsProperty
      AndCondition.ToObjectReturnType.FiltersListFieldType.IsnullFieldType getIsnull();

      @JsProperty
      AndCondition.ToObjectReturnType.FiltersListFieldType.MatchesFieldType getMatches();

      @JsProperty
      AndCondition.ToObjectReturnType.FiltersListFieldType.NotFieldType getNot();

      @JsProperty
      AndCondition.ToObjectReturnType.FiltersListFieldType.OrFieldType getOr();

      @JsProperty
      AndCondition.ToObjectReturnType.FiltersListFieldType.Pb_inFieldType getPb_in();

      @JsProperty
      AndCondition.ToObjectReturnType.FiltersListFieldType.SearchFieldType getSearch();

      @JsProperty
      void setAnd(Object and);

      @JsProperty
      void setCompare(
          AndCondition.ToObjectReturnType.FiltersListFieldType.CompareFieldType compare);

      @JsProperty
      void setContains(
          AndCondition.ToObjectReturnType.FiltersListFieldType.ContainsFieldType contains);

      @JsProperty
      void setInvoke(AndCondition.ToObjectReturnType.FiltersListFieldType.InvokeFieldType invoke);

      @JsProperty
      void setIsnull(AndCondition.ToObjectReturnType.FiltersListFieldType.IsnullFieldType isnull);

      @JsProperty
      void setMatches(
          AndCondition.ToObjectReturnType.FiltersListFieldType.MatchesFieldType matches);

      @JsProperty
      void setNot(AndCondition.ToObjectReturnType.FiltersListFieldType.NotFieldType not);

      @JsProperty
      void setOr(AndCondition.ToObjectReturnType.FiltersListFieldType.OrFieldType or);

      @JsProperty
      void setPb_in(AndCondition.ToObjectReturnType.FiltersListFieldType.Pb_inFieldType pb_in);

      @JsProperty
      void setSearch(AndCondition.ToObjectReturnType.FiltersListFieldType.SearchFieldType search);
    }

    @JsOverlay
    static AndCondition.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<AndCondition.ToObjectReturnType.FiltersListFieldType> getFiltersList();

    @JsOverlay
    default void setFiltersList(
        AndCondition.ToObjectReturnType.FiltersListFieldType[] filtersList) {
      setFiltersList(
          Js.<JsArray<AndCondition.ToObjectReturnType.FiltersListFieldType>>uncheckedCast(
              filtersList));
    }

    @JsProperty
    void setFiltersList(JsArray<AndCondition.ToObjectReturnType.FiltersListFieldType> filtersList);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface FiltersListFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface CompareFieldType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface LhsFieldType {
          @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
          public interface LiteralFieldType {
            @JsOverlay
            static AndCondition.ToObjectReturnType0.FiltersListFieldType.CompareFieldType
                    .LhsFieldType.LiteralFieldType
                create() {
              return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getDoublevalue();

            @JsProperty
            double getLongvalue();

            @JsProperty
            double getNanotimevalue();

            @JsProperty
            String getStringvalue();

            @JsProperty
            boolean isBoolvalue();

            @JsProperty
            void setBoolvalue(boolean boolvalue);

            @JsProperty
            void setDoublevalue(double doublevalue);

            @JsProperty
            void setLongvalue(double longvalue);

            @JsProperty
            void setNanotimevalue(double nanotimevalue);

            @JsProperty
            void setStringvalue(String stringvalue);
          }

          @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
          public interface ReferenceFieldType {
            @JsOverlay
            static AndCondition.ToObjectReturnType0.FiltersListFieldType.CompareFieldType
                    .LhsFieldType.ReferenceFieldType
                create() {
              return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getColumnname();

            @JsProperty
            void setColumnname(String columnname);
          }

          @JsOverlay
          static AndCondition.ToObjectReturnType0.FiltersListFieldType.CompareFieldType.LhsFieldType
              create() {
            return Js.uncheckedCast(JsPropertyMap.of());
          }

          @JsProperty
          AndCondition.ToObjectReturnType0.FiltersListFieldType.CompareFieldType.LhsFieldType
                  .LiteralFieldType
              getLiteral();

          @JsProperty
          AndCondition.ToObjectReturnType0.FiltersListFieldType.CompareFieldType.LhsFieldType
                  .ReferenceFieldType
              getReference();

          @JsProperty
          void setLiteral(
              AndCondition.ToObjectReturnType0.FiltersListFieldType.CompareFieldType.LhsFieldType
                      .LiteralFieldType
                  literal);

          @JsProperty
          void setReference(
              AndCondition.ToObjectReturnType0.FiltersListFieldType.CompareFieldType.LhsFieldType
                      .ReferenceFieldType
                  reference);
        }

        @JsOverlay
        static AndCondition.ToObjectReturnType0.FiltersListFieldType.CompareFieldType create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        AndCondition.ToObjectReturnType0.FiltersListFieldType.CompareFieldType.LhsFieldType
            getLhs();

        @JsProperty
        double getOperation();

        @JsProperty
        Object getRhs();

        @JsProperty
        void setLhs(
            AndCondition.ToObjectReturnType0.FiltersListFieldType.CompareFieldType.LhsFieldType
                lhs);

        @JsProperty
        void setOperation(double operation);

        @JsProperty
        void setRhs(Object rhs);
      }

      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface ContainsFieldType {
        @JsOverlay
        static AndCondition.ToObjectReturnType0.FiltersListFieldType.ContainsFieldType create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getCasesensitivity();

        @JsProperty
        double getMatchtype();

        @JsProperty
        Object getReference();

        @JsProperty
        String getSearchstring();

        @JsProperty
        void setCasesensitivity(double casesensitivity);

        @JsProperty
        void setMatchtype(double matchtype);

        @JsProperty
        void setReference(Object reference);

        @JsProperty
        void setSearchstring(String searchstring);
      }

      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface InvokeFieldType {
        @JsOverlay
        static AndCondition.ToObjectReturnType0.FiltersListFieldType.InvokeFieldType create() {
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
      public interface IsnullFieldType {
        @JsOverlay
        static AndCondition.ToObjectReturnType0.FiltersListFieldType.IsnullFieldType create() {
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
        static AndCondition.ToObjectReturnType0.FiltersListFieldType.MatchesFieldType create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getCasesensitivity();

        @JsProperty
        double getMatchtype();

        @JsProperty
        Object getReference();

        @JsProperty
        String getRegex();

        @JsProperty
        void setCasesensitivity(double casesensitivity);

        @JsProperty
        void setMatchtype(double matchtype);

        @JsProperty
        void setReference(Object reference);

        @JsProperty
        void setRegex(String regex);
      }

      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface NotFieldType {
        @JsOverlay
        static AndCondition.ToObjectReturnType0.FiltersListFieldType.NotFieldType create() {
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
        static AndCondition.ToObjectReturnType0.FiltersListFieldType.OrFieldType create() {
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
        static AndCondition.ToObjectReturnType0.FiltersListFieldType.Pb_inFieldType create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<Object> getCandidatesList();

        @JsProperty
        double getCasesensitivity();

        @JsProperty
        double getMatchtype();

        @JsProperty
        Object getTarget();

        @JsProperty
        void setCandidatesList(JsArray<Object> candidatesList);

        @JsOverlay
        default void setCandidatesList(Object[] candidatesList) {
          setCandidatesList(Js.<JsArray<Object>>uncheckedCast(candidatesList));
        }

        @JsProperty
        void setCasesensitivity(double casesensitivity);

        @JsProperty
        void setMatchtype(double matchtype);

        @JsProperty
        void setTarget(Object target);
      }

      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface SearchFieldType {
        @JsOverlay
        static AndCondition.ToObjectReturnType0.FiltersListFieldType.SearchFieldType create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<Object> getOptionalreferencesList();

        @JsProperty
        String getSearchstring();

        @JsProperty
        void setOptionalreferencesList(JsArray<Object> optionalreferencesList);

        @JsOverlay
        default void setOptionalreferencesList(Object[] optionalreferencesList) {
          setOptionalreferencesList(Js.<JsArray<Object>>uncheckedCast(optionalreferencesList));
        }

        @JsProperty
        void setSearchstring(String searchstring);
      }

      @JsOverlay
      static AndCondition.ToObjectReturnType0.FiltersListFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      Object getAnd();

      @JsProperty
      AndCondition.ToObjectReturnType0.FiltersListFieldType.CompareFieldType getCompare();

      @JsProperty
      AndCondition.ToObjectReturnType0.FiltersListFieldType.ContainsFieldType getContains();

      @JsProperty
      AndCondition.ToObjectReturnType0.FiltersListFieldType.InvokeFieldType getInvoke();

      @JsProperty
      AndCondition.ToObjectReturnType0.FiltersListFieldType.IsnullFieldType getIsnull();

      @JsProperty
      AndCondition.ToObjectReturnType0.FiltersListFieldType.MatchesFieldType getMatches();

      @JsProperty
      AndCondition.ToObjectReturnType0.FiltersListFieldType.NotFieldType getNot();

      @JsProperty
      AndCondition.ToObjectReturnType0.FiltersListFieldType.OrFieldType getOr();

      @JsProperty
      AndCondition.ToObjectReturnType0.FiltersListFieldType.Pb_inFieldType getPb_in();

      @JsProperty
      AndCondition.ToObjectReturnType0.FiltersListFieldType.SearchFieldType getSearch();

      @JsProperty
      void setAnd(Object and);

      @JsProperty
      void setCompare(
          AndCondition.ToObjectReturnType0.FiltersListFieldType.CompareFieldType compare);

      @JsProperty
      void setContains(
          AndCondition.ToObjectReturnType0.FiltersListFieldType.ContainsFieldType contains);

      @JsProperty
      void setInvoke(AndCondition.ToObjectReturnType0.FiltersListFieldType.InvokeFieldType invoke);

      @JsProperty
      void setIsnull(AndCondition.ToObjectReturnType0.FiltersListFieldType.IsnullFieldType isnull);

      @JsProperty
      void setMatches(
          AndCondition.ToObjectReturnType0.FiltersListFieldType.MatchesFieldType matches);

      @JsProperty
      void setNot(AndCondition.ToObjectReturnType0.FiltersListFieldType.NotFieldType not);

      @JsProperty
      void setOr(AndCondition.ToObjectReturnType0.FiltersListFieldType.OrFieldType or);

      @JsProperty
      void setPb_in(AndCondition.ToObjectReturnType0.FiltersListFieldType.Pb_inFieldType pb_in);

      @JsProperty
      void setSearch(AndCondition.ToObjectReturnType0.FiltersListFieldType.SearchFieldType search);
    }

    @JsOverlay
    static AndCondition.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<AndCondition.ToObjectReturnType0.FiltersListFieldType> getFiltersList();

    @JsOverlay
    default void setFiltersList(
        AndCondition.ToObjectReturnType0.FiltersListFieldType[] filtersList) {
      setFiltersList(
          Js.<JsArray<AndCondition.ToObjectReturnType0.FiltersListFieldType>>uncheckedCast(
              filtersList));
    }

    @JsProperty
    void setFiltersList(JsArray<AndCondition.ToObjectReturnType0.FiltersListFieldType> filtersList);
  }

  public static native AndCondition deserializeBinary(Uint8Array bytes);

  public static native AndCondition deserializeBinaryFromReader(
      AndCondition message, Object reader);

  public static native void serializeBinaryToWriter(AndCondition message, Object writer);

  public static native AndCondition.ToObjectReturnType toObject(
      boolean includeInstance, AndCondition msg);

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

  public native AndCondition.ToObjectReturnType0 toObject();

  public native AndCondition.ToObjectReturnType0 toObject(boolean includeInstance);
}
