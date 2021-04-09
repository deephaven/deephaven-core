package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb.Ticket;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.table_pb.FilterTableRequest",
    namespace = JsPackage.GLOBAL)
public class FilterTableRequest {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface FiltersListFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface AndFieldType {
        @JsOverlay
        static FilterTableRequest.ToObjectReturnType.FiltersListFieldType.AndFieldType create() {
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
            static FilterTableRequest.ToObjectReturnType.FiltersListFieldType.CompareFieldType
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
            static FilterTableRequest.ToObjectReturnType.FiltersListFieldType.CompareFieldType
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
          static FilterTableRequest.ToObjectReturnType.FiltersListFieldType.CompareFieldType
                  .LhsFieldType
              create() {
            return Js.uncheckedCast(JsPropertyMap.of());
          }

          @JsProperty
          FilterTableRequest.ToObjectReturnType.FiltersListFieldType.CompareFieldType.LhsFieldType
                  .LiteralFieldType
              getLiteral();

          @JsProperty
          FilterTableRequest.ToObjectReturnType.FiltersListFieldType.CompareFieldType.LhsFieldType
                  .ReferenceFieldType
              getReference();

          @JsProperty
          void setLiteral(
              FilterTableRequest.ToObjectReturnType.FiltersListFieldType.CompareFieldType
                      .LhsFieldType.LiteralFieldType
                  literal);

          @JsProperty
          void setReference(
              FilterTableRequest.ToObjectReturnType.FiltersListFieldType.CompareFieldType
                      .LhsFieldType.ReferenceFieldType
                  reference);
        }

        @JsOverlay
        static FilterTableRequest.ToObjectReturnType.FiltersListFieldType.CompareFieldType
            create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        FilterTableRequest.ToObjectReturnType.FiltersListFieldType.CompareFieldType.LhsFieldType
            getLhs();

        @JsProperty
        double getOperation();

        @JsProperty
        Object getRhs();

        @JsProperty
        void setLhs(
            FilterTableRequest.ToObjectReturnType.FiltersListFieldType.CompareFieldType.LhsFieldType
                lhs);

        @JsProperty
        void setOperation(double operation);

        @JsProperty
        void setRhs(Object rhs);
      }

      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface ContainsFieldType {
        @JsOverlay
        static FilterTableRequest.ToObjectReturnType.FiltersListFieldType.ContainsFieldType
            create() {
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
        static FilterTableRequest.ToObjectReturnType.FiltersListFieldType.InvokeFieldType create() {
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
        static FilterTableRequest.ToObjectReturnType.FiltersListFieldType.IsnullFieldType create() {
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
        static FilterTableRequest.ToObjectReturnType.FiltersListFieldType.MatchesFieldType
            create() {
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
        static FilterTableRequest.ToObjectReturnType.FiltersListFieldType.NotFieldType create() {
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
        static FilterTableRequest.ToObjectReturnType.FiltersListFieldType.OrFieldType create() {
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
        static FilterTableRequest.ToObjectReturnType.FiltersListFieldType.Pb_inFieldType create() {
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
        static FilterTableRequest.ToObjectReturnType.FiltersListFieldType.SearchFieldType create() {
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
      static FilterTableRequest.ToObjectReturnType.FiltersListFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      FilterTableRequest.ToObjectReturnType.FiltersListFieldType.AndFieldType getAnd();

      @JsProperty
      FilterTableRequest.ToObjectReturnType.FiltersListFieldType.CompareFieldType getCompare();

      @JsProperty
      FilterTableRequest.ToObjectReturnType.FiltersListFieldType.ContainsFieldType getContains();

      @JsProperty
      FilterTableRequest.ToObjectReturnType.FiltersListFieldType.InvokeFieldType getInvoke();

      @JsProperty
      FilterTableRequest.ToObjectReturnType.FiltersListFieldType.IsnullFieldType getIsnull();

      @JsProperty
      FilterTableRequest.ToObjectReturnType.FiltersListFieldType.MatchesFieldType getMatches();

      @JsProperty
      FilterTableRequest.ToObjectReturnType.FiltersListFieldType.NotFieldType getNot();

      @JsProperty
      FilterTableRequest.ToObjectReturnType.FiltersListFieldType.OrFieldType getOr();

      @JsProperty
      FilterTableRequest.ToObjectReturnType.FiltersListFieldType.Pb_inFieldType getPb_in();

      @JsProperty
      FilterTableRequest.ToObjectReturnType.FiltersListFieldType.SearchFieldType getSearch();

      @JsProperty
      void setAnd(FilterTableRequest.ToObjectReturnType.FiltersListFieldType.AndFieldType and);

      @JsProperty
      void setCompare(
          FilterTableRequest.ToObjectReturnType.FiltersListFieldType.CompareFieldType compare);

      @JsProperty
      void setContains(
          FilterTableRequest.ToObjectReturnType.FiltersListFieldType.ContainsFieldType contains);

      @JsProperty
      void setInvoke(
          FilterTableRequest.ToObjectReturnType.FiltersListFieldType.InvokeFieldType invoke);

      @JsProperty
      void setIsnull(
          FilterTableRequest.ToObjectReturnType.FiltersListFieldType.IsnullFieldType isnull);

      @JsProperty
      void setMatches(
          FilterTableRequest.ToObjectReturnType.FiltersListFieldType.MatchesFieldType matches);

      @JsProperty
      void setNot(FilterTableRequest.ToObjectReturnType.FiltersListFieldType.NotFieldType not);

      @JsProperty
      void setOr(FilterTableRequest.ToObjectReturnType.FiltersListFieldType.OrFieldType or);

      @JsProperty
      void setPb_in(
          FilterTableRequest.ToObjectReturnType.FiltersListFieldType.Pb_inFieldType pb_in);

      @JsProperty
      void setSearch(
          FilterTableRequest.ToObjectReturnType.FiltersListFieldType.SearchFieldType search);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultIdFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static FilterTableRequest.ToObjectReturnType.ResultIdFieldType.GetIdUnionType of(Object o) {
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
      static FilterTableRequest.ToObjectReturnType.ResultIdFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      FilterTableRequest.ToObjectReturnType.ResultIdFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(FilterTableRequest.ToObjectReturnType.ResultIdFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<FilterTableRequest.ToObjectReturnType.ResultIdFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<FilterTableRequest.ToObjectReturnType.ResultIdFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SourceIdFieldType {
      @JsOverlay
      static FilterTableRequest.ToObjectReturnType.SourceIdFieldType create() {
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
    static FilterTableRequest.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<FilterTableRequest.ToObjectReturnType.FiltersListFieldType> getFiltersList();

    @JsProperty
    FilterTableRequest.ToObjectReturnType.ResultIdFieldType getResultId();

    @JsProperty
    FilterTableRequest.ToObjectReturnType.SourceIdFieldType getSourceId();

    @JsOverlay
    default void setFiltersList(
        FilterTableRequest.ToObjectReturnType.FiltersListFieldType[] filtersList) {
      setFiltersList(
          Js.<JsArray<FilterTableRequest.ToObjectReturnType.FiltersListFieldType>>uncheckedCast(
              filtersList));
    }

    @JsProperty
    void setFiltersList(
        JsArray<FilterTableRequest.ToObjectReturnType.FiltersListFieldType> filtersList);

    @JsProperty
    void setResultId(FilterTableRequest.ToObjectReturnType.ResultIdFieldType resultId);

    @JsProperty
    void setSourceId(FilterTableRequest.ToObjectReturnType.SourceIdFieldType sourceId);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface FiltersListFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface AndFieldType {
        @JsOverlay
        static FilterTableRequest.ToObjectReturnType0.FiltersListFieldType.AndFieldType create() {
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
            static FilterTableRequest.ToObjectReturnType0.FiltersListFieldType.CompareFieldType
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
            static FilterTableRequest.ToObjectReturnType0.FiltersListFieldType.CompareFieldType
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
          static FilterTableRequest.ToObjectReturnType0.FiltersListFieldType.CompareFieldType
                  .LhsFieldType
              create() {
            return Js.uncheckedCast(JsPropertyMap.of());
          }

          @JsProperty
          FilterTableRequest.ToObjectReturnType0.FiltersListFieldType.CompareFieldType.LhsFieldType
                  .LiteralFieldType
              getLiteral();

          @JsProperty
          FilterTableRequest.ToObjectReturnType0.FiltersListFieldType.CompareFieldType.LhsFieldType
                  .ReferenceFieldType
              getReference();

          @JsProperty
          void setLiteral(
              FilterTableRequest.ToObjectReturnType0.FiltersListFieldType.CompareFieldType
                      .LhsFieldType.LiteralFieldType
                  literal);

          @JsProperty
          void setReference(
              FilterTableRequest.ToObjectReturnType0.FiltersListFieldType.CompareFieldType
                      .LhsFieldType.ReferenceFieldType
                  reference);
        }

        @JsOverlay
        static FilterTableRequest.ToObjectReturnType0.FiltersListFieldType.CompareFieldType
            create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        FilterTableRequest.ToObjectReturnType0.FiltersListFieldType.CompareFieldType.LhsFieldType
            getLhs();

        @JsProperty
        double getOperation();

        @JsProperty
        Object getRhs();

        @JsProperty
        void setLhs(
            FilterTableRequest.ToObjectReturnType0.FiltersListFieldType.CompareFieldType
                    .LhsFieldType
                lhs);

        @JsProperty
        void setOperation(double operation);

        @JsProperty
        void setRhs(Object rhs);
      }

      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface ContainsFieldType {
        @JsOverlay
        static FilterTableRequest.ToObjectReturnType0.FiltersListFieldType.ContainsFieldType
            create() {
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
        static FilterTableRequest.ToObjectReturnType0.FiltersListFieldType.InvokeFieldType
            create() {
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
        static FilterTableRequest.ToObjectReturnType0.FiltersListFieldType.IsnullFieldType
            create() {
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
        static FilterTableRequest.ToObjectReturnType0.FiltersListFieldType.MatchesFieldType
            create() {
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
        static FilterTableRequest.ToObjectReturnType0.FiltersListFieldType.NotFieldType create() {
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
        static FilterTableRequest.ToObjectReturnType0.FiltersListFieldType.OrFieldType create() {
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
        static FilterTableRequest.ToObjectReturnType0.FiltersListFieldType.Pb_inFieldType create() {
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
        static FilterTableRequest.ToObjectReturnType0.FiltersListFieldType.SearchFieldType
            create() {
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
      static FilterTableRequest.ToObjectReturnType0.FiltersListFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      FilterTableRequest.ToObjectReturnType0.FiltersListFieldType.AndFieldType getAnd();

      @JsProperty
      FilterTableRequest.ToObjectReturnType0.FiltersListFieldType.CompareFieldType getCompare();

      @JsProperty
      FilterTableRequest.ToObjectReturnType0.FiltersListFieldType.ContainsFieldType getContains();

      @JsProperty
      FilterTableRequest.ToObjectReturnType0.FiltersListFieldType.InvokeFieldType getInvoke();

      @JsProperty
      FilterTableRequest.ToObjectReturnType0.FiltersListFieldType.IsnullFieldType getIsnull();

      @JsProperty
      FilterTableRequest.ToObjectReturnType0.FiltersListFieldType.MatchesFieldType getMatches();

      @JsProperty
      FilterTableRequest.ToObjectReturnType0.FiltersListFieldType.NotFieldType getNot();

      @JsProperty
      FilterTableRequest.ToObjectReturnType0.FiltersListFieldType.OrFieldType getOr();

      @JsProperty
      FilterTableRequest.ToObjectReturnType0.FiltersListFieldType.Pb_inFieldType getPb_in();

      @JsProperty
      FilterTableRequest.ToObjectReturnType0.FiltersListFieldType.SearchFieldType getSearch();

      @JsProperty
      void setAnd(FilterTableRequest.ToObjectReturnType0.FiltersListFieldType.AndFieldType and);

      @JsProperty
      void setCompare(
          FilterTableRequest.ToObjectReturnType0.FiltersListFieldType.CompareFieldType compare);

      @JsProperty
      void setContains(
          FilterTableRequest.ToObjectReturnType0.FiltersListFieldType.ContainsFieldType contains);

      @JsProperty
      void setInvoke(
          FilterTableRequest.ToObjectReturnType0.FiltersListFieldType.InvokeFieldType invoke);

      @JsProperty
      void setIsnull(
          FilterTableRequest.ToObjectReturnType0.FiltersListFieldType.IsnullFieldType isnull);

      @JsProperty
      void setMatches(
          FilterTableRequest.ToObjectReturnType0.FiltersListFieldType.MatchesFieldType matches);

      @JsProperty
      void setNot(FilterTableRequest.ToObjectReturnType0.FiltersListFieldType.NotFieldType not);

      @JsProperty
      void setOr(FilterTableRequest.ToObjectReturnType0.FiltersListFieldType.OrFieldType or);

      @JsProperty
      void setPb_in(
          FilterTableRequest.ToObjectReturnType0.FiltersListFieldType.Pb_inFieldType pb_in);

      @JsProperty
      void setSearch(
          FilterTableRequest.ToObjectReturnType0.FiltersListFieldType.SearchFieldType search);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultIdFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static FilterTableRequest.ToObjectReturnType0.ResultIdFieldType.GetIdUnionType of(
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
      static FilterTableRequest.ToObjectReturnType0.ResultIdFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      FilterTableRequest.ToObjectReturnType0.ResultIdFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(FilterTableRequest.ToObjectReturnType0.ResultIdFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<FilterTableRequest.ToObjectReturnType0.ResultIdFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<FilterTableRequest.ToObjectReturnType0.ResultIdFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SourceIdFieldType {
      @JsOverlay
      static FilterTableRequest.ToObjectReturnType0.SourceIdFieldType create() {
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
    static FilterTableRequest.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<FilterTableRequest.ToObjectReturnType0.FiltersListFieldType> getFiltersList();

    @JsProperty
    FilterTableRequest.ToObjectReturnType0.ResultIdFieldType getResultId();

    @JsProperty
    FilterTableRequest.ToObjectReturnType0.SourceIdFieldType getSourceId();

    @JsOverlay
    default void setFiltersList(
        FilterTableRequest.ToObjectReturnType0.FiltersListFieldType[] filtersList) {
      setFiltersList(
          Js.<JsArray<FilterTableRequest.ToObjectReturnType0.FiltersListFieldType>>uncheckedCast(
              filtersList));
    }

    @JsProperty
    void setFiltersList(
        JsArray<FilterTableRequest.ToObjectReturnType0.FiltersListFieldType> filtersList);

    @JsProperty
    void setResultId(FilterTableRequest.ToObjectReturnType0.ResultIdFieldType resultId);

    @JsProperty
    void setSourceId(FilterTableRequest.ToObjectReturnType0.SourceIdFieldType sourceId);
  }

  public static native FilterTableRequest deserializeBinary(Uint8Array bytes);

  public static native FilterTableRequest deserializeBinaryFromReader(
      FilterTableRequest message, Object reader);

  public static native void serializeBinaryToWriter(FilterTableRequest message, Object writer);

  public static native FilterTableRequest.ToObjectReturnType toObject(
      boolean includeInstance, FilterTableRequest msg);

  public native Condition addFilters();

  public native Condition addFilters(Condition value, double index);

  public native Condition addFilters(Condition value);

  public native void clearFiltersList();

  public native void clearResultId();

  public native void clearSourceId();

  public native JsArray<Condition> getFiltersList();

  public native Ticket getResultId();

  public native TableReference getSourceId();

  public native boolean hasResultId();

  public native boolean hasSourceId();

  public native Uint8Array serializeBinary();

  @JsOverlay
  public final void setFiltersList(Condition[] value) {
    setFiltersList(Js.<JsArray<Condition>>uncheckedCast(value));
  }

  public native void setFiltersList(JsArray<Condition> value);

  public native void setResultId();

  public native void setResultId(Ticket value);

  public native void setSourceId();

  public native void setSourceId(TableReference value);

  public native FilterTableRequest.ToObjectReturnType0 toObject();

  public native FilterTableRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
