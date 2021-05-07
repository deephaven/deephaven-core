package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.batchtablerequest;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.ComboAggregateRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.DropColumnsRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.EmptyTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.FilterTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.FlattenRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.HeadOrTailByRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.HeadOrTailRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.JoinTablesRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.MergeTablesRequest;
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
    public interface ComboaggregateFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface AggregatesListFieldType {
        @JsOverlay
        static Operation.ToObjectReturnType.ComboaggregateFieldType.AggregatesListFieldType
            create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getColumnname();

        @JsProperty
        JsArray<String> getMatchpairsList();

        @JsProperty
        double getPercentile();

        @JsProperty
        double getType();

        @JsProperty
        boolean isAvgmedian();

        @JsProperty
        void setAvgmedian(boolean avgmedian);

        @JsProperty
        void setColumnname(String columnname);

        @JsProperty
        void setMatchpairsList(JsArray<String> matchpairsList);

        @JsOverlay
        default void setMatchpairsList(String[] matchpairsList) {
          setMatchpairsList(Js.<JsArray<String>>uncheckedCast(matchpairsList));
        }

        @JsProperty
        void setPercentile(double percentile);

        @JsProperty
        void setType(double type);
      }

      @JsOverlay
      static Operation.ToObjectReturnType.ComboaggregateFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      JsArray<Operation.ToObjectReturnType.ComboaggregateFieldType.AggregatesListFieldType>
          getAggregatesList();

      @JsProperty
      JsArray<String> getGroupbycolumnsList();

      @JsProperty
      Object getResultid();

      @JsProperty
      Object getSourceid();

      @JsProperty
      boolean isForcecombo();

      @JsOverlay
      default void setAggregatesList(
          Operation.ToObjectReturnType.ComboaggregateFieldType.AggregatesListFieldType[]
              aggregatesList) {
        setAggregatesList(
            Js
                .<JsArray<
                        Operation.ToObjectReturnType.ComboaggregateFieldType
                            .AggregatesListFieldType>>
                    uncheckedCast(aggregatesList));
      }

      @JsProperty
      void setAggregatesList(
          JsArray<Operation.ToObjectReturnType.ComboaggregateFieldType.AggregatesListFieldType>
              aggregatesList);

      @JsProperty
      void setForcecombo(boolean forcecombo);

      @JsProperty
      void setGroupbycolumnsList(JsArray<String> groupbycolumnsList);

      @JsOverlay
      default void setGroupbycolumnsList(String[] groupbycolumnsList) {
        setGroupbycolumnsList(Js.<JsArray<String>>uncheckedCast(groupbycolumnsList));
      }

      @JsProperty
      void setResultid(Object resultid);

      @JsProperty
      void setSourceid(Object sourceid);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface DropcolumnsFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface SourceidFieldType {
        @JsOverlay
        static Operation.ToObjectReturnType.DropcolumnsFieldType.SourceidFieldType create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getBatchoffset();

        @JsProperty
        Object getTicket();

        @JsProperty
        void setBatchoffset(double batchoffset);

        @JsProperty
        void setTicket(Object ticket);
      }

      @JsOverlay
      static Operation.ToObjectReturnType.DropcolumnsFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      JsArray<String> getColumnnamesList();

      @JsProperty
      Object getResultid();

      @JsProperty
      Operation.ToObjectReturnType.DropcolumnsFieldType.SourceidFieldType getSourceid();

      @JsProperty
      void setColumnnamesList(JsArray<String> columnnamesList);

      @JsOverlay
      default void setColumnnamesList(String[] columnnamesList) {
        setColumnnamesList(Js.<JsArray<String>>uncheckedCast(columnnamesList));
      }

      @JsProperty
      void setResultid(Object resultid);

      @JsProperty
      void setSourceid(
          Operation.ToObjectReturnType.DropcolumnsFieldType.SourceidFieldType sourceid);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface EmptytableFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface ResultidFieldType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetIdUnionType {
          @JsOverlay
          static Operation.ToObjectReturnType.EmptytableFieldType.ResultidFieldType.GetIdUnionType
              of(Object o) {
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
        static Operation.ToObjectReturnType.EmptytableFieldType.ResultidFieldType create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Operation.ToObjectReturnType.EmptytableFieldType.ResultidFieldType.GetIdUnionType getId();

        @JsProperty
        void setId(
            Operation.ToObjectReturnType.EmptytableFieldType.ResultidFieldType.GetIdUnionType id);

        @JsOverlay
        default void setId(String id) {
          setId(
              Js
                  .<Operation.ToObjectReturnType.EmptytableFieldType.ResultidFieldType
                          .GetIdUnionType>
                      uncheckedCast(id));
        }

        @JsOverlay
        default void setId(Uint8Array id) {
          setId(
              Js
                  .<Operation.ToObjectReturnType.EmptytableFieldType.ResultidFieldType
                          .GetIdUnionType>
                      uncheckedCast(id));
        }
      }

      @JsOverlay
      static Operation.ToObjectReturnType.EmptytableFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      JsArray<String> getColumnnamesList();

      @JsProperty
      JsArray<String> getColumntypesList();

      @JsProperty
      Operation.ToObjectReturnType.EmptytableFieldType.ResultidFieldType getResultid();

      @JsProperty
      String getSize();

      @JsProperty
      void setColumnnamesList(JsArray<String> columnnamesList);

      @JsOverlay
      default void setColumnnamesList(String[] columnnamesList) {
        setColumnnamesList(Js.<JsArray<String>>uncheckedCast(columnnamesList));
      }

      @JsProperty
      void setColumntypesList(JsArray<String> columntypesList);

      @JsOverlay
      default void setColumntypesList(String[] columntypesList) {
        setColumntypesList(Js.<JsArray<String>>uncheckedCast(columntypesList));
      }

      @JsProperty
      void setResultid(Operation.ToObjectReturnType.EmptytableFieldType.ResultidFieldType resultid);

      @JsProperty
      void setSize(String size);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface FilterFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface FiltersListFieldType {
        @JsOverlay
        static Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<Object> getChildrenList();

        @JsProperty
        double getDoublevalue();

        @JsProperty
        double getFilterop();

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
        void setChildrenList(JsArray<Object> childrenList);

        @JsOverlay
        default void setChildrenList(Object[] childrenList) {
          setChildrenList(Js.<JsArray<Object>>uncheckedCast(childrenList));
        }

        @JsProperty
        void setDoublevalue(double doublevalue);

        @JsProperty
        void setFilterop(double filterop);

        @JsProperty
        void setLongvalue(double longvalue);

        @JsProperty
        void setNanotimevalue(double nanotimevalue);

        @JsProperty
        void setStringvalue(String stringvalue);
      }

      @JsOverlay
      static Operation.ToObjectReturnType.FilterFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      JsArray<Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType> getFiltersList();

      @JsProperty
      Object getResultid();

      @JsProperty
      Object getSourceid();

      @JsOverlay
      default void setFiltersList(
          Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType[] filtersList) {
        setFiltersList(
            Js
                .<JsArray<Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType>>
                    uncheckedCast(filtersList));
      }

      @JsProperty
      void setFiltersList(
          JsArray<Operation.ToObjectReturnType.FilterFieldType.FiltersListFieldType> filtersList);

      @JsProperty
      void setResultid(Object resultid);

      @JsProperty
      void setSourceid(Object sourceid);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface FlattenFieldType {
      @JsOverlay
      static Operation.ToObjectReturnType.FlattenFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      Object getResultid();

      @JsProperty
      Object getSourceid();

      @JsProperty
      void setResultid(Object resultid);

      @JsProperty
      void setSourceid(Object sourceid);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface HeadFieldType {
      @JsOverlay
      static Operation.ToObjectReturnType.HeadFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      String getNumrows();

      @JsProperty
      Object getResultid();

      @JsProperty
      Object getSourceid();

      @JsProperty
      void setNumrows(String numrows);

      @JsProperty
      void setResultid(Object resultid);

      @JsProperty
      void setSourceid(Object sourceid);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface HeadbyFieldType {
      @JsOverlay
      static Operation.ToObjectReturnType.HeadbyFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      JsArray<String> getGroupbycolumnspecsList();

      @JsProperty
      String getNumrows();

      @JsProperty
      Object getResultid();

      @JsProperty
      Object getSourceid();

      @JsProperty
      void setGroupbycolumnspecsList(JsArray<String> groupbycolumnspecsList);

      @JsOverlay
      default void setGroupbycolumnspecsList(String[] groupbycolumnspecsList) {
        setGroupbycolumnspecsList(Js.<JsArray<String>>uncheckedCast(groupbycolumnspecsList));
      }

      @JsProperty
      void setNumrows(String numrows);

      @JsProperty
      void setResultid(Object resultid);

      @JsProperty
      void setSourceid(Object sourceid);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface JoinFieldType {
      @JsOverlay
      static Operation.ToObjectReturnType.JoinFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      JsArray<String> getColumnstoaddList();

      @JsProperty
      JsArray<String> getColumnstomatchList();

      @JsProperty
      double getJointype();

      @JsProperty
      Object getLeftid();

      @JsProperty
      Object getResultid();

      @JsProperty
      Object getRightid();

      @JsProperty
      void setColumnstoaddList(JsArray<String> columnstoaddList);

      @JsOverlay
      default void setColumnstoaddList(String[] columnstoaddList) {
        setColumnstoaddList(Js.<JsArray<String>>uncheckedCast(columnstoaddList));
      }

      @JsProperty
      void setColumnstomatchList(JsArray<String> columnstomatchList);

      @JsOverlay
      default void setColumnstomatchList(String[] columnstomatchList) {
        setColumnstomatchList(Js.<JsArray<String>>uncheckedCast(columnstomatchList));
      }

      @JsProperty
      void setJointype(double jointype);

      @JsProperty
      void setLeftid(Object leftid);

      @JsProperty
      void setResultid(Object resultid);

      @JsProperty
      void setRightid(Object rightid);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface MergeFieldType {
      @JsOverlay
      static Operation.ToObjectReturnType.MergeFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      String getKeycolumn();

      @JsProperty
      Object getResultid();

      @JsProperty
      JsArray<Object> getSourceidsList();

      @JsProperty
      void setKeycolumn(String keycolumn);

      @JsProperty
      void setResultid(Object resultid);

      @JsProperty
      void setSourceidsList(JsArray<Object> sourceidsList);

      @JsOverlay
      default void setSourceidsList(Object[] sourceidsList) {
        setSourceidsList(Js.<JsArray<Object>>uncheckedCast(sourceidsList));
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SelectdistinctFieldType {
      @JsOverlay
      static Operation.ToObjectReturnType.SelectdistinctFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      JsArray<String> getColumnnamesList();

      @JsProperty
      Object getResultid();

      @JsProperty
      Object getSourceid();

      @JsProperty
      void setColumnnamesList(JsArray<String> columnnamesList);

      @JsOverlay
      default void setColumnnamesList(String[] columnnamesList) {
        setColumnnamesList(Js.<JsArray<String>>uncheckedCast(columnnamesList));
      }

      @JsProperty
      void setResultid(Object resultid);

      @JsProperty
      void setSourceid(Object sourceid);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SnapshotFieldType {
      @JsOverlay
      static Operation.ToObjectReturnType.SnapshotFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      Object getLeftid();

      @JsProperty
      Object getResultid();

      @JsProperty
      Object getRightid();

      @JsProperty
      JsArray<String> getStampcolumnsList();

      @JsProperty
      boolean isDoinitialsnapshot();

      @JsProperty
      void setDoinitialsnapshot(boolean doinitialsnapshot);

      @JsProperty
      void setLeftid(Object leftid);

      @JsProperty
      void setResultid(Object resultid);

      @JsProperty
      void setRightid(Object rightid);

      @JsProperty
      void setStampcolumnsList(JsArray<String> stampcolumnsList);

      @JsOverlay
      default void setStampcolumnsList(String[] stampcolumnsList) {
        setStampcolumnsList(Js.<JsArray<String>>uncheckedCast(stampcolumnsList));
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
        String getColumnname();

        @JsProperty
        double getDirection();

        @JsProperty
        boolean isIsabsolute();

        @JsProperty
        void setColumnname(String columnname);

        @JsProperty
        void setDirection(double direction);

        @JsProperty
        void setIsabsolute(boolean isabsolute);
      }

      @JsOverlay
      static Operation.ToObjectReturnType.SortFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      Object getResultid();

      @JsProperty
      JsArray<Operation.ToObjectReturnType.SortFieldType.SortsListFieldType> getSortsList();

      @JsProperty
      Object getSourceid();

      @JsProperty
      void setResultid(Object resultid);

      @JsProperty
      void setSortsList(
          JsArray<Operation.ToObjectReturnType.SortFieldType.SortsListFieldType> sortsList);

      @JsOverlay
      default void setSortsList(
          Operation.ToObjectReturnType.SortFieldType.SortsListFieldType[] sortsList) {
        setSortsList(
            Js
                .<JsArray<Operation.ToObjectReturnType.SortFieldType.SortsListFieldType>>
                    uncheckedCast(sortsList));
      }

      @JsProperty
      void setSourceid(Object sourceid);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface TimetableFieldType {
      @JsOverlay
      static Operation.ToObjectReturnType.TimetableFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      String getPeriodnanos();

      @JsProperty
      Object getResultid();

      @JsProperty
      String getStarttimenanos();

      @JsProperty
      void setPeriodnanos(String periodnanos);

      @JsProperty
      void setResultid(Object resultid);

      @JsProperty
      void setStarttimenanos(String starttimenanos);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface UngroupFieldType {
      @JsOverlay
      static Operation.ToObjectReturnType.UngroupFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      JsArray<String> getColumnstoungroupList();

      @JsProperty
      Object getResultid();

      @JsProperty
      Object getSourceid();

      @JsProperty
      boolean isNullfill();

      @JsProperty
      void setColumnstoungroupList(JsArray<String> columnstoungroupList);

      @JsOverlay
      default void setColumnstoungroupList(String[] columnstoungroupList) {
        setColumnstoungroupList(Js.<JsArray<String>>uncheckedCast(columnstoungroupList));
      }

      @JsProperty
      void setNullfill(boolean nullfill);

      @JsProperty
      void setResultid(Object resultid);

      @JsProperty
      void setSourceid(Object sourceid);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface UnstructuredfilterFieldType {
      @JsOverlay
      static Operation.ToObjectReturnType.UnstructuredfilterFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      JsArray<String> getFilterList();

      @JsProperty
      Object getResultid();

      @JsProperty
      Object getSourceid();

      @JsProperty
      void setFilterList(JsArray<String> filterList);

      @JsOverlay
      default void setFilterList(String[] filterList) {
        setFilterList(Js.<JsArray<String>>uncheckedCast(filterList));
      }

      @JsProperty
      void setResultid(Object resultid);

      @JsProperty
      void setSourceid(Object sourceid);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface UpdateFieldType {
      @JsOverlay
      static Operation.ToObjectReturnType.UpdateFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      JsArray<String> getColumnspecsList();

      @JsProperty
      Object getResultid();

      @JsProperty
      Object getSourceid();

      @JsProperty
      void setColumnspecsList(JsArray<String> columnspecsList);

      @JsOverlay
      default void setColumnspecsList(String[] columnspecsList) {
        setColumnspecsList(Js.<JsArray<String>>uncheckedCast(columnspecsList));
      }

      @JsProperty
      void setResultid(Object resultid);

      @JsProperty
      void setSourceid(Object sourceid);
    }

    @JsOverlay
    static Operation.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    Operation.ToObjectReturnType.ComboaggregateFieldType getComboaggregate();

    @JsProperty
    Operation.ToObjectReturnType.DropcolumnsFieldType getDropcolumns();

    @JsProperty
    Operation.ToObjectReturnType.EmptytableFieldType getEmptytable();

    @JsProperty
    Operation.ToObjectReturnType.FilterFieldType getFilter();

    @JsProperty
    Operation.ToObjectReturnType.FlattenFieldType getFlatten();

    @JsProperty
    Operation.ToObjectReturnType.HeadFieldType getHead();

    @JsProperty
    Operation.ToObjectReturnType.HeadbyFieldType getHeadby();

    @JsProperty
    Operation.ToObjectReturnType.JoinFieldType getJoin();

    @JsProperty
    Object getLazyupdate();

    @JsProperty
    Operation.ToObjectReturnType.MergeFieldType getMerge();

    @JsProperty
    Object getSelect();

    @JsProperty
    Operation.ToObjectReturnType.SelectdistinctFieldType getSelectdistinct();

    @JsProperty
    Operation.ToObjectReturnType.SnapshotFieldType getSnapshot();

    @JsProperty
    Operation.ToObjectReturnType.SortFieldType getSort();

    @JsProperty
    Object getTail();

    @JsProperty
    Object getTailby();

    @JsProperty
    Operation.ToObjectReturnType.TimetableFieldType getTimetable();

    @JsProperty
    Operation.ToObjectReturnType.UngroupFieldType getUngroup();

    @JsProperty
    Operation.ToObjectReturnType.UnstructuredfilterFieldType getUnstructuredfilter();

    @JsProperty
    Operation.ToObjectReturnType.UpdateFieldType getUpdate();

    @JsProperty
    Object getUpdateview();

    @JsProperty
    Object getView();

    @JsProperty
    void setComboaggregate(Operation.ToObjectReturnType.ComboaggregateFieldType comboaggregate);

    @JsProperty
    void setDropcolumns(Operation.ToObjectReturnType.DropcolumnsFieldType dropcolumns);

    @JsProperty
    void setEmptytable(Operation.ToObjectReturnType.EmptytableFieldType emptytable);

    @JsProperty
    void setFilter(Operation.ToObjectReturnType.FilterFieldType filter);

    @JsProperty
    void setFlatten(Operation.ToObjectReturnType.FlattenFieldType flatten);

    @JsProperty
    void setHead(Operation.ToObjectReturnType.HeadFieldType head);

    @JsProperty
    void setHeadby(Operation.ToObjectReturnType.HeadbyFieldType headby);

    @JsProperty
    void setJoin(Operation.ToObjectReturnType.JoinFieldType join);

    @JsProperty
    void setLazyupdate(Object lazyupdate);

    @JsProperty
    void setMerge(Operation.ToObjectReturnType.MergeFieldType merge);

    @JsProperty
    void setSelect(Object select);

    @JsProperty
    void setSelectdistinct(Operation.ToObjectReturnType.SelectdistinctFieldType selectdistinct);

    @JsProperty
    void setSnapshot(Operation.ToObjectReturnType.SnapshotFieldType snapshot);

    @JsProperty
    void setSort(Operation.ToObjectReturnType.SortFieldType sort);

    @JsProperty
    void setTail(Object tail);

    @JsProperty
    void setTailby(Object tailby);

    @JsProperty
    void setTimetable(Operation.ToObjectReturnType.TimetableFieldType timetable);

    @JsProperty
    void setUngroup(Operation.ToObjectReturnType.UngroupFieldType ungroup);

    @JsProperty
    void setUnstructuredfilter(
        Operation.ToObjectReturnType.UnstructuredfilterFieldType unstructuredfilter);

    @JsProperty
    void setUpdate(Operation.ToObjectReturnType.UpdateFieldType update);

    @JsProperty
    void setUpdateview(Object updateview);

    @JsProperty
    void setView(Object view);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ComboaggregateFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface AggregatesListFieldType {
        @JsOverlay
        static Operation.ToObjectReturnType0.ComboaggregateFieldType.AggregatesListFieldType
            create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getColumnname();

        @JsProperty
        JsArray<String> getMatchpairsList();

        @JsProperty
        double getPercentile();

        @JsProperty
        double getType();

        @JsProperty
        boolean isAvgmedian();

        @JsProperty
        void setAvgmedian(boolean avgmedian);

        @JsProperty
        void setColumnname(String columnname);

        @JsProperty
        void setMatchpairsList(JsArray<String> matchpairsList);

        @JsOverlay
        default void setMatchpairsList(String[] matchpairsList) {
          setMatchpairsList(Js.<JsArray<String>>uncheckedCast(matchpairsList));
        }

        @JsProperty
        void setPercentile(double percentile);

        @JsProperty
        void setType(double type);
      }

      @JsOverlay
      static Operation.ToObjectReturnType0.ComboaggregateFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      JsArray<Operation.ToObjectReturnType0.ComboaggregateFieldType.AggregatesListFieldType>
          getAggregatesList();

      @JsProperty
      JsArray<String> getGroupbycolumnsList();

      @JsProperty
      Object getResultid();

      @JsProperty
      Object getSourceid();

      @JsProperty
      boolean isForcecombo();

      @JsOverlay
      default void setAggregatesList(
          Operation.ToObjectReturnType0.ComboaggregateFieldType.AggregatesListFieldType[]
              aggregatesList) {
        setAggregatesList(
            Js
                .<JsArray<
                        Operation.ToObjectReturnType0.ComboaggregateFieldType
                            .AggregatesListFieldType>>
                    uncheckedCast(aggregatesList));
      }

      @JsProperty
      void setAggregatesList(
          JsArray<Operation.ToObjectReturnType0.ComboaggregateFieldType.AggregatesListFieldType>
              aggregatesList);

      @JsProperty
      void setForcecombo(boolean forcecombo);

      @JsProperty
      void setGroupbycolumnsList(JsArray<String> groupbycolumnsList);

      @JsOverlay
      default void setGroupbycolumnsList(String[] groupbycolumnsList) {
        setGroupbycolumnsList(Js.<JsArray<String>>uncheckedCast(groupbycolumnsList));
      }

      @JsProperty
      void setResultid(Object resultid);

      @JsProperty
      void setSourceid(Object sourceid);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface DropcolumnsFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface SourceidFieldType {
        @JsOverlay
        static Operation.ToObjectReturnType0.DropcolumnsFieldType.SourceidFieldType create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getBatchoffset();

        @JsProperty
        Object getTicket();

        @JsProperty
        void setBatchoffset(double batchoffset);

        @JsProperty
        void setTicket(Object ticket);
      }

      @JsOverlay
      static Operation.ToObjectReturnType0.DropcolumnsFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      JsArray<String> getColumnnamesList();

      @JsProperty
      Object getResultid();

      @JsProperty
      Operation.ToObjectReturnType0.DropcolumnsFieldType.SourceidFieldType getSourceid();

      @JsProperty
      void setColumnnamesList(JsArray<String> columnnamesList);

      @JsOverlay
      default void setColumnnamesList(String[] columnnamesList) {
        setColumnnamesList(Js.<JsArray<String>>uncheckedCast(columnnamesList));
      }

      @JsProperty
      void setResultid(Object resultid);

      @JsProperty
      void setSourceid(
          Operation.ToObjectReturnType0.DropcolumnsFieldType.SourceidFieldType sourceid);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface EmptytableFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface ResultidFieldType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetIdUnionType {
          @JsOverlay
          static Operation.ToObjectReturnType0.EmptytableFieldType.ResultidFieldType.GetIdUnionType
              of(Object o) {
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
        static Operation.ToObjectReturnType0.EmptytableFieldType.ResultidFieldType create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Operation.ToObjectReturnType0.EmptytableFieldType.ResultidFieldType.GetIdUnionType getId();

        @JsProperty
        void setId(
            Operation.ToObjectReturnType0.EmptytableFieldType.ResultidFieldType.GetIdUnionType id);

        @JsOverlay
        default void setId(String id) {
          setId(
              Js
                  .<Operation.ToObjectReturnType0.EmptytableFieldType.ResultidFieldType
                          .GetIdUnionType>
                      uncheckedCast(id));
        }

        @JsOverlay
        default void setId(Uint8Array id) {
          setId(
              Js
                  .<Operation.ToObjectReturnType0.EmptytableFieldType.ResultidFieldType
                          .GetIdUnionType>
                      uncheckedCast(id));
        }
      }

      @JsOverlay
      static Operation.ToObjectReturnType0.EmptytableFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      JsArray<String> getColumnnamesList();

      @JsProperty
      JsArray<String> getColumntypesList();

      @JsProperty
      Operation.ToObjectReturnType0.EmptytableFieldType.ResultidFieldType getResultid();

      @JsProperty
      String getSize();

      @JsProperty
      void setColumnnamesList(JsArray<String> columnnamesList);

      @JsOverlay
      default void setColumnnamesList(String[] columnnamesList) {
        setColumnnamesList(Js.<JsArray<String>>uncheckedCast(columnnamesList));
      }

      @JsProperty
      void setColumntypesList(JsArray<String> columntypesList);

      @JsOverlay
      default void setColumntypesList(String[] columntypesList) {
        setColumntypesList(Js.<JsArray<String>>uncheckedCast(columntypesList));
      }

      @JsProperty
      void setResultid(
          Operation.ToObjectReturnType0.EmptytableFieldType.ResultidFieldType resultid);

      @JsProperty
      void setSize(String size);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface FilterFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface FiltersListFieldType {
        @JsOverlay
        static Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<Object> getChildrenList();

        @JsProperty
        double getDoublevalue();

        @JsProperty
        double getFilterop();

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
        void setChildrenList(JsArray<Object> childrenList);

        @JsOverlay
        default void setChildrenList(Object[] childrenList) {
          setChildrenList(Js.<JsArray<Object>>uncheckedCast(childrenList));
        }

        @JsProperty
        void setDoublevalue(double doublevalue);

        @JsProperty
        void setFilterop(double filterop);

        @JsProperty
        void setLongvalue(double longvalue);

        @JsProperty
        void setNanotimevalue(double nanotimevalue);

        @JsProperty
        void setStringvalue(String stringvalue);
      }

      @JsOverlay
      static Operation.ToObjectReturnType0.FilterFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      JsArray<Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType> getFiltersList();

      @JsProperty
      Object getResultid();

      @JsProperty
      Object getSourceid();

      @JsOverlay
      default void setFiltersList(
          Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType[] filtersList) {
        setFiltersList(
            Js
                .<JsArray<Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType>>
                    uncheckedCast(filtersList));
      }

      @JsProperty
      void setFiltersList(
          JsArray<Operation.ToObjectReturnType0.FilterFieldType.FiltersListFieldType> filtersList);

      @JsProperty
      void setResultid(Object resultid);

      @JsProperty
      void setSourceid(Object sourceid);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface FlattenFieldType {
      @JsOverlay
      static Operation.ToObjectReturnType0.FlattenFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      Object getResultid();

      @JsProperty
      Object getSourceid();

      @JsProperty
      void setResultid(Object resultid);

      @JsProperty
      void setSourceid(Object sourceid);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface HeadFieldType {
      @JsOverlay
      static Operation.ToObjectReturnType0.HeadFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      String getNumrows();

      @JsProperty
      Object getResultid();

      @JsProperty
      Object getSourceid();

      @JsProperty
      void setNumrows(String numrows);

      @JsProperty
      void setResultid(Object resultid);

      @JsProperty
      void setSourceid(Object sourceid);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface HeadbyFieldType {
      @JsOverlay
      static Operation.ToObjectReturnType0.HeadbyFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      JsArray<String> getGroupbycolumnspecsList();

      @JsProperty
      String getNumrows();

      @JsProperty
      Object getResultid();

      @JsProperty
      Object getSourceid();

      @JsProperty
      void setGroupbycolumnspecsList(JsArray<String> groupbycolumnspecsList);

      @JsOverlay
      default void setGroupbycolumnspecsList(String[] groupbycolumnspecsList) {
        setGroupbycolumnspecsList(Js.<JsArray<String>>uncheckedCast(groupbycolumnspecsList));
      }

      @JsProperty
      void setNumrows(String numrows);

      @JsProperty
      void setResultid(Object resultid);

      @JsProperty
      void setSourceid(Object sourceid);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface JoinFieldType {
      @JsOverlay
      static Operation.ToObjectReturnType0.JoinFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      JsArray<String> getColumnstoaddList();

      @JsProperty
      JsArray<String> getColumnstomatchList();

      @JsProperty
      double getJointype();

      @JsProperty
      Object getLeftid();

      @JsProperty
      Object getResultid();

      @JsProperty
      Object getRightid();

      @JsProperty
      void setColumnstoaddList(JsArray<String> columnstoaddList);

      @JsOverlay
      default void setColumnstoaddList(String[] columnstoaddList) {
        setColumnstoaddList(Js.<JsArray<String>>uncheckedCast(columnstoaddList));
      }

      @JsProperty
      void setColumnstomatchList(JsArray<String> columnstomatchList);

      @JsOverlay
      default void setColumnstomatchList(String[] columnstomatchList) {
        setColumnstomatchList(Js.<JsArray<String>>uncheckedCast(columnstomatchList));
      }

      @JsProperty
      void setJointype(double jointype);

      @JsProperty
      void setLeftid(Object leftid);

      @JsProperty
      void setResultid(Object resultid);

      @JsProperty
      void setRightid(Object rightid);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface MergeFieldType {
      @JsOverlay
      static Operation.ToObjectReturnType0.MergeFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      String getKeycolumn();

      @JsProperty
      Object getResultid();

      @JsProperty
      JsArray<Object> getSourceidsList();

      @JsProperty
      void setKeycolumn(String keycolumn);

      @JsProperty
      void setResultid(Object resultid);

      @JsProperty
      void setSourceidsList(JsArray<Object> sourceidsList);

      @JsOverlay
      default void setSourceidsList(Object[] sourceidsList) {
        setSourceidsList(Js.<JsArray<Object>>uncheckedCast(sourceidsList));
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SelectdistinctFieldType {
      @JsOverlay
      static Operation.ToObjectReturnType0.SelectdistinctFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      JsArray<String> getColumnnamesList();

      @JsProperty
      Object getResultid();

      @JsProperty
      Object getSourceid();

      @JsProperty
      void setColumnnamesList(JsArray<String> columnnamesList);

      @JsOverlay
      default void setColumnnamesList(String[] columnnamesList) {
        setColumnnamesList(Js.<JsArray<String>>uncheckedCast(columnnamesList));
      }

      @JsProperty
      void setResultid(Object resultid);

      @JsProperty
      void setSourceid(Object sourceid);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SnapshotFieldType {
      @JsOverlay
      static Operation.ToObjectReturnType0.SnapshotFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      Object getLeftid();

      @JsProperty
      Object getResultid();

      @JsProperty
      Object getRightid();

      @JsProperty
      JsArray<String> getStampcolumnsList();

      @JsProperty
      boolean isDoinitialsnapshot();

      @JsProperty
      void setDoinitialsnapshot(boolean doinitialsnapshot);

      @JsProperty
      void setLeftid(Object leftid);

      @JsProperty
      void setResultid(Object resultid);

      @JsProperty
      void setRightid(Object rightid);

      @JsProperty
      void setStampcolumnsList(JsArray<String> stampcolumnsList);

      @JsOverlay
      default void setStampcolumnsList(String[] stampcolumnsList) {
        setStampcolumnsList(Js.<JsArray<String>>uncheckedCast(stampcolumnsList));
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
        String getColumnname();

        @JsProperty
        double getDirection();

        @JsProperty
        boolean isIsabsolute();

        @JsProperty
        void setColumnname(String columnname);

        @JsProperty
        void setDirection(double direction);

        @JsProperty
        void setIsabsolute(boolean isabsolute);
      }

      @JsOverlay
      static Operation.ToObjectReturnType0.SortFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      Object getResultid();

      @JsProperty
      JsArray<Operation.ToObjectReturnType0.SortFieldType.SortsListFieldType> getSortsList();

      @JsProperty
      Object getSourceid();

      @JsProperty
      void setResultid(Object resultid);

      @JsProperty
      void setSortsList(
          JsArray<Operation.ToObjectReturnType0.SortFieldType.SortsListFieldType> sortsList);

      @JsOverlay
      default void setSortsList(
          Operation.ToObjectReturnType0.SortFieldType.SortsListFieldType[] sortsList) {
        setSortsList(
            Js
                .<JsArray<Operation.ToObjectReturnType0.SortFieldType.SortsListFieldType>>
                    uncheckedCast(sortsList));
      }

      @JsProperty
      void setSourceid(Object sourceid);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface TimetableFieldType {
      @JsOverlay
      static Operation.ToObjectReturnType0.TimetableFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      String getPeriodnanos();

      @JsProperty
      Object getResultid();

      @JsProperty
      String getStarttimenanos();

      @JsProperty
      void setPeriodnanos(String periodnanos);

      @JsProperty
      void setResultid(Object resultid);

      @JsProperty
      void setStarttimenanos(String starttimenanos);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface UngroupFieldType {
      @JsOverlay
      static Operation.ToObjectReturnType0.UngroupFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      JsArray<String> getColumnstoungroupList();

      @JsProperty
      Object getResultid();

      @JsProperty
      Object getSourceid();

      @JsProperty
      boolean isNullfill();

      @JsProperty
      void setColumnstoungroupList(JsArray<String> columnstoungroupList);

      @JsOverlay
      default void setColumnstoungroupList(String[] columnstoungroupList) {
        setColumnstoungroupList(Js.<JsArray<String>>uncheckedCast(columnstoungroupList));
      }

      @JsProperty
      void setNullfill(boolean nullfill);

      @JsProperty
      void setResultid(Object resultid);

      @JsProperty
      void setSourceid(Object sourceid);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface UnstructuredfilterFieldType {
      @JsOverlay
      static Operation.ToObjectReturnType0.UnstructuredfilterFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      JsArray<String> getFilterList();

      @JsProperty
      Object getResultid();

      @JsProperty
      Object getSourceid();

      @JsProperty
      void setFilterList(JsArray<String> filterList);

      @JsOverlay
      default void setFilterList(String[] filterList) {
        setFilterList(Js.<JsArray<String>>uncheckedCast(filterList));
      }

      @JsProperty
      void setResultid(Object resultid);

      @JsProperty
      void setSourceid(Object sourceid);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface UpdateFieldType {
      @JsOverlay
      static Operation.ToObjectReturnType0.UpdateFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      JsArray<String> getColumnspecsList();

      @JsProperty
      Object getResultid();

      @JsProperty
      Object getSourceid();

      @JsProperty
      void setColumnspecsList(JsArray<String> columnspecsList);

      @JsOverlay
      default void setColumnspecsList(String[] columnspecsList) {
        setColumnspecsList(Js.<JsArray<String>>uncheckedCast(columnspecsList));
      }

      @JsProperty
      void setResultid(Object resultid);

      @JsProperty
      void setSourceid(Object sourceid);
    }

    @JsOverlay
    static Operation.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    Operation.ToObjectReturnType0.ComboaggregateFieldType getComboaggregate();

    @JsProperty
    Operation.ToObjectReturnType0.DropcolumnsFieldType getDropcolumns();

    @JsProperty
    Operation.ToObjectReturnType0.EmptytableFieldType getEmptytable();

    @JsProperty
    Operation.ToObjectReturnType0.FilterFieldType getFilter();

    @JsProperty
    Operation.ToObjectReturnType0.FlattenFieldType getFlatten();

    @JsProperty
    Operation.ToObjectReturnType0.HeadFieldType getHead();

    @JsProperty
    Operation.ToObjectReturnType0.HeadbyFieldType getHeadby();

    @JsProperty
    Operation.ToObjectReturnType0.JoinFieldType getJoin();

    @JsProperty
    Object getLazyupdate();

    @JsProperty
    Operation.ToObjectReturnType0.MergeFieldType getMerge();

    @JsProperty
    Object getSelect();

    @JsProperty
    Operation.ToObjectReturnType0.SelectdistinctFieldType getSelectdistinct();

    @JsProperty
    Operation.ToObjectReturnType0.SnapshotFieldType getSnapshot();

    @JsProperty
    Operation.ToObjectReturnType0.SortFieldType getSort();

    @JsProperty
    Object getTail();

    @JsProperty
    Object getTailby();

    @JsProperty
    Operation.ToObjectReturnType0.TimetableFieldType getTimetable();

    @JsProperty
    Operation.ToObjectReturnType0.UngroupFieldType getUngroup();

    @JsProperty
    Operation.ToObjectReturnType0.UnstructuredfilterFieldType getUnstructuredfilter();

    @JsProperty
    Operation.ToObjectReturnType0.UpdateFieldType getUpdate();

    @JsProperty
    Object getUpdateview();

    @JsProperty
    Object getView();

    @JsProperty
    void setComboaggregate(Operation.ToObjectReturnType0.ComboaggregateFieldType comboaggregate);

    @JsProperty
    void setDropcolumns(Operation.ToObjectReturnType0.DropcolumnsFieldType dropcolumns);

    @JsProperty
    void setEmptytable(Operation.ToObjectReturnType0.EmptytableFieldType emptytable);

    @JsProperty
    void setFilter(Operation.ToObjectReturnType0.FilterFieldType filter);

    @JsProperty
    void setFlatten(Operation.ToObjectReturnType0.FlattenFieldType flatten);

    @JsProperty
    void setHead(Operation.ToObjectReturnType0.HeadFieldType head);

    @JsProperty
    void setHeadby(Operation.ToObjectReturnType0.HeadbyFieldType headby);

    @JsProperty
    void setJoin(Operation.ToObjectReturnType0.JoinFieldType join);

    @JsProperty
    void setLazyupdate(Object lazyupdate);

    @JsProperty
    void setMerge(Operation.ToObjectReturnType0.MergeFieldType merge);

    @JsProperty
    void setSelect(Object select);

    @JsProperty
    void setSelectdistinct(Operation.ToObjectReturnType0.SelectdistinctFieldType selectdistinct);

    @JsProperty
    void setSnapshot(Operation.ToObjectReturnType0.SnapshotFieldType snapshot);

    @JsProperty
    void setSort(Operation.ToObjectReturnType0.SortFieldType sort);

    @JsProperty
    void setTail(Object tail);

    @JsProperty
    void setTailby(Object tailby);

    @JsProperty
    void setTimetable(Operation.ToObjectReturnType0.TimetableFieldType timetable);

    @JsProperty
    void setUngroup(Operation.ToObjectReturnType0.UngroupFieldType ungroup);

    @JsProperty
    void setUnstructuredfilter(
        Operation.ToObjectReturnType0.UnstructuredfilterFieldType unstructuredfilter);

    @JsProperty
    void setUpdate(Operation.ToObjectReturnType0.UpdateFieldType update);

    @JsProperty
    void setUpdateview(Object updateview);

    @JsProperty
    void setView(Object view);
  }

  public static native Operation deserializeBinary(Uint8Array bytes);

  public static native Operation deserializeBinaryFromReader(Operation message, Object reader);

  public static native void serializeBinaryToWriter(Operation message, Object writer);

  public static native Operation.ToObjectReturnType toObject(
      boolean includeInstance, Operation msg);

  public native void clearComboaggregate();

  public native void clearDropcolumns();

  public native void clearEmptytable();

  public native void clearFilter();

  public native void clearFlatten();

  public native void clearHead();

  public native void clearHeadby();

  public native void clearJoin();

  public native void clearLazyupdate();

  public native void clearMerge();

  public native void clearSelect();

  public native void clearSelectdistinct();

  public native void clearSnapshot();

  public native void clearSort();

  public native void clearTail();

  public native void clearTailby();

  public native void clearTimetable();

  public native void clearUngroup();

  public native void clearUnstructuredfilter();

  public native void clearUpdate();

  public native void clearUpdateview();

  public native void clearView();

  public native ComboAggregateRequest getComboaggregate();

  public native DropColumnsRequest getDropcolumns();

  public native EmptyTableRequest getEmptytable();

  public native FilterTableRequest getFilter();

  public native FlattenRequest getFlatten();

  public native HeadOrTailRequest getHead();

  public native HeadOrTailByRequest getHeadby();

  public native JoinTablesRequest getJoin();

  public native SelectOrUpdateRequest getLazyupdate();

  public native MergeTablesRequest getMerge();

  public native int getOpCase();

  public native SelectOrUpdateRequest getSelect();

  public native SelectDistinctRequest getSelectdistinct();

  public native SnapshotTableRequest getSnapshot();

  public native SortTableRequest getSort();

  public native HeadOrTailRequest getTail();

  public native HeadOrTailByRequest getTailby();

  public native TimeTableRequest getTimetable();

  public native UngroupRequest getUngroup();

  public native UnstructuredFilterTableRequest getUnstructuredfilter();

  public native SelectOrUpdateRequest getUpdate();

  public native SelectOrUpdateRequest getUpdateview();

  public native SelectOrUpdateRequest getView();

  public native boolean hasComboaggregate();

  public native boolean hasDropcolumns();

  public native boolean hasEmptytable();

  public native boolean hasFilter();

  public native boolean hasFlatten();

  public native boolean hasHead();

  public native boolean hasHeadby();

  public native boolean hasJoin();

  public native boolean hasLazyupdate();

  public native boolean hasMerge();

  public native boolean hasSelect();

  public native boolean hasSelectdistinct();

  public native boolean hasSnapshot();

  public native boolean hasSort();

  public native boolean hasTail();

  public native boolean hasTailby();

  public native boolean hasTimetable();

  public native boolean hasUngroup();

  public native boolean hasUnstructuredfilter();

  public native boolean hasUpdate();

  public native boolean hasUpdateview();

  public native boolean hasView();

  public native Uint8Array serializeBinary();

  public native void setComboaggregate();

  public native void setComboaggregate(ComboAggregateRequest value);

  public native void setDropcolumns();

  public native void setDropcolumns(DropColumnsRequest value);

  public native void setEmptytable();

  public native void setEmptytable(EmptyTableRequest value);

  public native void setFilter();

  public native void setFilter(FilterTableRequest value);

  public native void setFlatten();

  public native void setFlatten(FlattenRequest value);

  public native void setHead();

  public native void setHead(HeadOrTailRequest value);

  public native void setHeadby();

  public native void setHeadby(HeadOrTailByRequest value);

  public native void setJoin();

  public native void setJoin(JoinTablesRequest value);

  public native void setLazyupdate();

  public native void setLazyupdate(SelectOrUpdateRequest value);

  public native void setMerge();

  public native void setMerge(MergeTablesRequest value);

  public native void setSelect();

  public native void setSelect(SelectOrUpdateRequest value);

  public native void setSelectdistinct();

  public native void setSelectdistinct(SelectDistinctRequest value);

  public native void setSnapshot();

  public native void setSnapshot(SnapshotTableRequest value);

  public native void setSort();

  public native void setSort(SortTableRequest value);

  public native void setTail();

  public native void setTail(HeadOrTailRequest value);

  public native void setTailby();

  public native void setTailby(HeadOrTailByRequest value);

  public native void setTimetable();

  public native void setTimetable(TimeTableRequest value);

  public native void setUngroup();

  public native void setUngroup(UngroupRequest value);

  public native void setUnstructuredfilter();

  public native void setUnstructuredfilter(UnstructuredFilterTableRequest value);

  public native void setUpdate();

  public native void setUpdate(SelectOrUpdateRequest value);

  public native void setUpdateview();

  public native void setUpdateview(SelectOrUpdateRequest value);

  public native void setView();

  public native void setView(SelectOrUpdateRequest value);

  public native Operation.ToObjectReturnType0 toObject();

  public native Operation.ToObjectReturnType0 toObject(boolean includeInstance);
}
