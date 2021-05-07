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
    public interface OpListFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface ComboaggregateFieldType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface AggregatesListFieldType {
          @JsOverlay
          static BatchTableRequest.ToObjectReturnType.OpListFieldType.ComboaggregateFieldType
                  .AggregatesListFieldType
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
        static BatchTableRequest.ToObjectReturnType.OpListFieldType.ComboaggregateFieldType
            create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<
                BatchTableRequest.ToObjectReturnType.OpListFieldType.ComboaggregateFieldType
                    .AggregatesListFieldType>
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
            BatchTableRequest.ToObjectReturnType.OpListFieldType.ComboaggregateFieldType
                        .AggregatesListFieldType
                    []
                aggregatesList) {
          setAggregatesList(
              Js
                  .<JsArray<
                          BatchTableRequest.ToObjectReturnType.OpListFieldType
                              .ComboaggregateFieldType.AggregatesListFieldType>>
                      uncheckedCast(aggregatesList));
        }

        @JsProperty
        void setAggregatesList(
            JsArray<
                    BatchTableRequest.ToObjectReturnType.OpListFieldType.ComboaggregateFieldType
                        .AggregatesListFieldType>
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
          static BatchTableRequest.ToObjectReturnType.OpListFieldType.DropcolumnsFieldType
                  .SourceidFieldType
              create() {
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
        static BatchTableRequest.ToObjectReturnType.OpListFieldType.DropcolumnsFieldType create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<String> getColumnnamesList();

        @JsProperty
        Object getResultid();

        @JsProperty
        BatchTableRequest.ToObjectReturnType.OpListFieldType.DropcolumnsFieldType.SourceidFieldType
            getSourceid();

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
            BatchTableRequest.ToObjectReturnType.OpListFieldType.DropcolumnsFieldType
                    .SourceidFieldType
                sourceid);
      }

      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface EmptytableFieldType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultidFieldType {
          @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
          public interface GetIdUnionType {
            @JsOverlay
            static BatchTableRequest.ToObjectReturnType.OpListFieldType.EmptytableFieldType
                    .ResultidFieldType.GetIdUnionType
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
          static BatchTableRequest.ToObjectReturnType.OpListFieldType.EmptytableFieldType
                  .ResultidFieldType
              create() {
            return Js.uncheckedCast(JsPropertyMap.of());
          }

          @JsProperty
          BatchTableRequest.ToObjectReturnType.OpListFieldType.EmptytableFieldType.ResultidFieldType
                  .GetIdUnionType
              getId();

          @JsProperty
          void setId(
              BatchTableRequest.ToObjectReturnType.OpListFieldType.EmptytableFieldType
                      .ResultidFieldType.GetIdUnionType
                  id);

          @JsOverlay
          default void setId(String id) {
            setId(
                Js
                    .<BatchTableRequest.ToObjectReturnType.OpListFieldType.EmptytableFieldType
                            .ResultidFieldType.GetIdUnionType>
                        uncheckedCast(id));
          }

          @JsOverlay
          default void setId(Uint8Array id) {
            setId(
                Js
                    .<BatchTableRequest.ToObjectReturnType.OpListFieldType.EmptytableFieldType
                            .ResultidFieldType.GetIdUnionType>
                        uncheckedCast(id));
          }
        }

        @JsOverlay
        static BatchTableRequest.ToObjectReturnType.OpListFieldType.EmptytableFieldType create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<String> getColumnnamesList();

        @JsProperty
        JsArray<String> getColumntypesList();

        @JsProperty
        BatchTableRequest.ToObjectReturnType.OpListFieldType.EmptytableFieldType.ResultidFieldType
            getResultid();

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
            BatchTableRequest.ToObjectReturnType.OpListFieldType.EmptytableFieldType
                    .ResultidFieldType
                resultid);

        @JsProperty
        void setSize(String size);
      }

      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface FilterFieldType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface FiltersListFieldType {
          @JsOverlay
          static BatchTableRequest.ToObjectReturnType.OpListFieldType.FilterFieldType
                  .FiltersListFieldType
              create() {
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
        static BatchTableRequest.ToObjectReturnType.OpListFieldType.FilterFieldType create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<
                BatchTableRequest.ToObjectReturnType.OpListFieldType.FilterFieldType
                    .FiltersListFieldType>
            getFiltersList();

        @JsProperty
        Object getResultid();

        @JsProperty
        Object getSourceid();

        @JsOverlay
        default void setFiltersList(
            BatchTableRequest.ToObjectReturnType.OpListFieldType.FilterFieldType
                        .FiltersListFieldType
                    []
                filtersList) {
          setFiltersList(
              Js
                  .<JsArray<
                          BatchTableRequest.ToObjectReturnType.OpListFieldType.FilterFieldType
                              .FiltersListFieldType>>
                      uncheckedCast(filtersList));
        }

        @JsProperty
        void setFiltersList(
            JsArray<
                    BatchTableRequest.ToObjectReturnType.OpListFieldType.FilterFieldType
                        .FiltersListFieldType>
                filtersList);

        @JsProperty
        void setResultid(Object resultid);

        @JsProperty
        void setSourceid(Object sourceid);
      }

      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface FlattenFieldType {
        @JsOverlay
        static BatchTableRequest.ToObjectReturnType.OpListFieldType.FlattenFieldType create() {
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
        static BatchTableRequest.ToObjectReturnType.OpListFieldType.HeadFieldType create() {
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
        static BatchTableRequest.ToObjectReturnType.OpListFieldType.HeadbyFieldType create() {
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
        static BatchTableRequest.ToObjectReturnType.OpListFieldType.JoinFieldType create() {
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
        static BatchTableRequest.ToObjectReturnType.OpListFieldType.MergeFieldType create() {
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
        static BatchTableRequest.ToObjectReturnType.OpListFieldType.SelectdistinctFieldType
            create() {
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
        static BatchTableRequest.ToObjectReturnType.OpListFieldType.SnapshotFieldType create() {
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
          static BatchTableRequest.ToObjectReturnType.OpListFieldType.SortFieldType
                  .SortsListFieldType
              create() {
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
        static BatchTableRequest.ToObjectReturnType.OpListFieldType.SortFieldType create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Object getResultid();

        @JsProperty
        JsArray<
                BatchTableRequest.ToObjectReturnType.OpListFieldType.SortFieldType
                    .SortsListFieldType>
            getSortsList();

        @JsProperty
        Object getSourceid();

        @JsProperty
        void setResultid(Object resultid);

        @JsProperty
        void setSortsList(
            JsArray<
                    BatchTableRequest.ToObjectReturnType.OpListFieldType.SortFieldType
                        .SortsListFieldType>
                sortsList);

        @JsOverlay
        default void setSortsList(
            BatchTableRequest.ToObjectReturnType.OpListFieldType.SortFieldType.SortsListFieldType[]
                sortsList) {
          setSortsList(
              Js
                  .<JsArray<
                          BatchTableRequest.ToObjectReturnType.OpListFieldType.SortFieldType
                              .SortsListFieldType>>
                      uncheckedCast(sortsList));
        }

        @JsProperty
        void setSourceid(Object sourceid);
      }

      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface TimetableFieldType {
        @JsOverlay
        static BatchTableRequest.ToObjectReturnType.OpListFieldType.TimetableFieldType create() {
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
        static BatchTableRequest.ToObjectReturnType.OpListFieldType.UngroupFieldType create() {
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
        static BatchTableRequest.ToObjectReturnType.OpListFieldType.UnstructuredfilterFieldType
            create() {
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
        static BatchTableRequest.ToObjectReturnType.OpListFieldType.UpdateFieldType create() {
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
      static BatchTableRequest.ToObjectReturnType.OpListFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      BatchTableRequest.ToObjectReturnType.OpListFieldType.ComboaggregateFieldType
          getComboaggregate();

      @JsProperty
      BatchTableRequest.ToObjectReturnType.OpListFieldType.DropcolumnsFieldType getDropcolumns();

      @JsProperty
      BatchTableRequest.ToObjectReturnType.OpListFieldType.EmptytableFieldType getEmptytable();

      @JsProperty
      BatchTableRequest.ToObjectReturnType.OpListFieldType.FilterFieldType getFilter();

      @JsProperty
      BatchTableRequest.ToObjectReturnType.OpListFieldType.FlattenFieldType getFlatten();

      @JsProperty
      BatchTableRequest.ToObjectReturnType.OpListFieldType.HeadFieldType getHead();

      @JsProperty
      BatchTableRequest.ToObjectReturnType.OpListFieldType.HeadbyFieldType getHeadby();

      @JsProperty
      BatchTableRequest.ToObjectReturnType.OpListFieldType.JoinFieldType getJoin();

      @JsProperty
      Object getLazyupdate();

      @JsProperty
      BatchTableRequest.ToObjectReturnType.OpListFieldType.MergeFieldType getMerge();

      @JsProperty
      Object getSelect();

      @JsProperty
      BatchTableRequest.ToObjectReturnType.OpListFieldType.SelectdistinctFieldType
          getSelectdistinct();

      @JsProperty
      BatchTableRequest.ToObjectReturnType.OpListFieldType.SnapshotFieldType getSnapshot();

      @JsProperty
      BatchTableRequest.ToObjectReturnType.OpListFieldType.SortFieldType getSort();

      @JsProperty
      Object getTail();

      @JsProperty
      Object getTailby();

      @JsProperty
      BatchTableRequest.ToObjectReturnType.OpListFieldType.TimetableFieldType getTimetable();

      @JsProperty
      BatchTableRequest.ToObjectReturnType.OpListFieldType.UngroupFieldType getUngroup();

      @JsProperty
      BatchTableRequest.ToObjectReturnType.OpListFieldType.UnstructuredfilterFieldType
          getUnstructuredfilter();

      @JsProperty
      BatchTableRequest.ToObjectReturnType.OpListFieldType.UpdateFieldType getUpdate();

      @JsProperty
      Object getUpdateview();

      @JsProperty
      Object getView();

      @JsProperty
      void setComboaggregate(
          BatchTableRequest.ToObjectReturnType.OpListFieldType.ComboaggregateFieldType
              comboaggregate);

      @JsProperty
      void setDropcolumns(
          BatchTableRequest.ToObjectReturnType.OpListFieldType.DropcolumnsFieldType dropcolumns);

      @JsProperty
      void setEmptytable(
          BatchTableRequest.ToObjectReturnType.OpListFieldType.EmptytableFieldType emptytable);

      @JsProperty
      void setFilter(BatchTableRequest.ToObjectReturnType.OpListFieldType.FilterFieldType filter);

      @JsProperty
      void setFlatten(
          BatchTableRequest.ToObjectReturnType.OpListFieldType.FlattenFieldType flatten);

      @JsProperty
      void setHead(BatchTableRequest.ToObjectReturnType.OpListFieldType.HeadFieldType head);

      @JsProperty
      void setHeadby(BatchTableRequest.ToObjectReturnType.OpListFieldType.HeadbyFieldType headby);

      @JsProperty
      void setJoin(BatchTableRequest.ToObjectReturnType.OpListFieldType.JoinFieldType join);

      @JsProperty
      void setLazyupdate(Object lazyupdate);

      @JsProperty
      void setMerge(BatchTableRequest.ToObjectReturnType.OpListFieldType.MergeFieldType merge);

      @JsProperty
      void setSelect(Object select);

      @JsProperty
      void setSelectdistinct(
          BatchTableRequest.ToObjectReturnType.OpListFieldType.SelectdistinctFieldType
              selectdistinct);

      @JsProperty
      void setSnapshot(
          BatchTableRequest.ToObjectReturnType.OpListFieldType.SnapshotFieldType snapshot);

      @JsProperty
      void setSort(BatchTableRequest.ToObjectReturnType.OpListFieldType.SortFieldType sort);

      @JsProperty
      void setTail(Object tail);

      @JsProperty
      void setTailby(Object tailby);

      @JsProperty
      void setTimetable(
          BatchTableRequest.ToObjectReturnType.OpListFieldType.TimetableFieldType timetable);

      @JsProperty
      void setUngroup(
          BatchTableRequest.ToObjectReturnType.OpListFieldType.UngroupFieldType ungroup);

      @JsProperty
      void setUnstructuredfilter(
          BatchTableRequest.ToObjectReturnType.OpListFieldType.UnstructuredfilterFieldType
              unstructuredfilter);

      @JsProperty
      void setUpdate(BatchTableRequest.ToObjectReturnType.OpListFieldType.UpdateFieldType update);

      @JsProperty
      void setUpdateview(Object updateview);

      @JsProperty
      void setView(Object view);
    }

    @JsOverlay
    static BatchTableRequest.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<BatchTableRequest.ToObjectReturnType.OpListFieldType> getOpList();

    @JsProperty
    void setOpList(JsArray<BatchTableRequest.ToObjectReturnType.OpListFieldType> opList);

    @JsOverlay
    default void setOpList(BatchTableRequest.ToObjectReturnType.OpListFieldType[] opList) {
      setOpList(
          Js.<JsArray<BatchTableRequest.ToObjectReturnType.OpListFieldType>>uncheckedCast(opList));
    }
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface OpListFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface ComboaggregateFieldType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface AggregatesListFieldType {
          @JsOverlay
          static BatchTableRequest.ToObjectReturnType0.OpListFieldType.ComboaggregateFieldType
                  .AggregatesListFieldType
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
        static BatchTableRequest.ToObjectReturnType0.OpListFieldType.ComboaggregateFieldType
            create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<
                BatchTableRequest.ToObjectReturnType0.OpListFieldType.ComboaggregateFieldType
                    .AggregatesListFieldType>
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
            BatchTableRequest.ToObjectReturnType0.OpListFieldType.ComboaggregateFieldType
                        .AggregatesListFieldType
                    []
                aggregatesList) {
          setAggregatesList(
              Js
                  .<JsArray<
                          BatchTableRequest.ToObjectReturnType0.OpListFieldType
                              .ComboaggregateFieldType.AggregatesListFieldType>>
                      uncheckedCast(aggregatesList));
        }

        @JsProperty
        void setAggregatesList(
            JsArray<
                    BatchTableRequest.ToObjectReturnType0.OpListFieldType.ComboaggregateFieldType
                        .AggregatesListFieldType>
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
          static BatchTableRequest.ToObjectReturnType0.OpListFieldType.DropcolumnsFieldType
                  .SourceidFieldType
              create() {
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
        static BatchTableRequest.ToObjectReturnType0.OpListFieldType.DropcolumnsFieldType create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<String> getColumnnamesList();

        @JsProperty
        Object getResultid();

        @JsProperty
        BatchTableRequest.ToObjectReturnType0.OpListFieldType.DropcolumnsFieldType.SourceidFieldType
            getSourceid();

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
            BatchTableRequest.ToObjectReturnType0.OpListFieldType.DropcolumnsFieldType
                    .SourceidFieldType
                sourceid);
      }

      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface EmptytableFieldType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultidFieldType {
          @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
          public interface GetIdUnionType {
            @JsOverlay
            static BatchTableRequest.ToObjectReturnType0.OpListFieldType.EmptytableFieldType
                    .ResultidFieldType.GetIdUnionType
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
          static BatchTableRequest.ToObjectReturnType0.OpListFieldType.EmptytableFieldType
                  .ResultidFieldType
              create() {
            return Js.uncheckedCast(JsPropertyMap.of());
          }

          @JsProperty
          BatchTableRequest.ToObjectReturnType0.OpListFieldType.EmptytableFieldType
                  .ResultidFieldType.GetIdUnionType
              getId();

          @JsProperty
          void setId(
              BatchTableRequest.ToObjectReturnType0.OpListFieldType.EmptytableFieldType
                      .ResultidFieldType.GetIdUnionType
                  id);

          @JsOverlay
          default void setId(String id) {
            setId(
                Js
                    .<BatchTableRequest.ToObjectReturnType0.OpListFieldType.EmptytableFieldType
                            .ResultidFieldType.GetIdUnionType>
                        uncheckedCast(id));
          }

          @JsOverlay
          default void setId(Uint8Array id) {
            setId(
                Js
                    .<BatchTableRequest.ToObjectReturnType0.OpListFieldType.EmptytableFieldType
                            .ResultidFieldType.GetIdUnionType>
                        uncheckedCast(id));
          }
        }

        @JsOverlay
        static BatchTableRequest.ToObjectReturnType0.OpListFieldType.EmptytableFieldType create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<String> getColumnnamesList();

        @JsProperty
        JsArray<String> getColumntypesList();

        @JsProperty
        BatchTableRequest.ToObjectReturnType0.OpListFieldType.EmptytableFieldType.ResultidFieldType
            getResultid();

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
            BatchTableRequest.ToObjectReturnType0.OpListFieldType.EmptytableFieldType
                    .ResultidFieldType
                resultid);

        @JsProperty
        void setSize(String size);
      }

      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface FilterFieldType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface FiltersListFieldType {
          @JsOverlay
          static BatchTableRequest.ToObjectReturnType0.OpListFieldType.FilterFieldType
                  .FiltersListFieldType
              create() {
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
        static BatchTableRequest.ToObjectReturnType0.OpListFieldType.FilterFieldType create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<
                BatchTableRequest.ToObjectReturnType0.OpListFieldType.FilterFieldType
                    .FiltersListFieldType>
            getFiltersList();

        @JsProperty
        Object getResultid();

        @JsProperty
        Object getSourceid();

        @JsOverlay
        default void setFiltersList(
            BatchTableRequest.ToObjectReturnType0.OpListFieldType.FilterFieldType
                        .FiltersListFieldType
                    []
                filtersList) {
          setFiltersList(
              Js
                  .<JsArray<
                          BatchTableRequest.ToObjectReturnType0.OpListFieldType.FilterFieldType
                              .FiltersListFieldType>>
                      uncheckedCast(filtersList));
        }

        @JsProperty
        void setFiltersList(
            JsArray<
                    BatchTableRequest.ToObjectReturnType0.OpListFieldType.FilterFieldType
                        .FiltersListFieldType>
                filtersList);

        @JsProperty
        void setResultid(Object resultid);

        @JsProperty
        void setSourceid(Object sourceid);
      }

      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface FlattenFieldType {
        @JsOverlay
        static BatchTableRequest.ToObjectReturnType0.OpListFieldType.FlattenFieldType create() {
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
        static BatchTableRequest.ToObjectReturnType0.OpListFieldType.HeadFieldType create() {
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
        static BatchTableRequest.ToObjectReturnType0.OpListFieldType.HeadbyFieldType create() {
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
        static BatchTableRequest.ToObjectReturnType0.OpListFieldType.JoinFieldType create() {
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
        static BatchTableRequest.ToObjectReturnType0.OpListFieldType.MergeFieldType create() {
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
        static BatchTableRequest.ToObjectReturnType0.OpListFieldType.SelectdistinctFieldType
            create() {
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
        static BatchTableRequest.ToObjectReturnType0.OpListFieldType.SnapshotFieldType create() {
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
          static BatchTableRequest.ToObjectReturnType0.OpListFieldType.SortFieldType
                  .SortsListFieldType
              create() {
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
        static BatchTableRequest.ToObjectReturnType0.OpListFieldType.SortFieldType create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Object getResultid();

        @JsProperty
        JsArray<
                BatchTableRequest.ToObjectReturnType0.OpListFieldType.SortFieldType
                    .SortsListFieldType>
            getSortsList();

        @JsProperty
        Object getSourceid();

        @JsProperty
        void setResultid(Object resultid);

        @JsProperty
        void setSortsList(
            JsArray<
                    BatchTableRequest.ToObjectReturnType0.OpListFieldType.SortFieldType
                        .SortsListFieldType>
                sortsList);

        @JsOverlay
        default void setSortsList(
            BatchTableRequest.ToObjectReturnType0.OpListFieldType.SortFieldType.SortsListFieldType[]
                sortsList) {
          setSortsList(
              Js
                  .<JsArray<
                          BatchTableRequest.ToObjectReturnType0.OpListFieldType.SortFieldType
                              .SortsListFieldType>>
                      uncheckedCast(sortsList));
        }

        @JsProperty
        void setSourceid(Object sourceid);
      }

      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface TimetableFieldType {
        @JsOverlay
        static BatchTableRequest.ToObjectReturnType0.OpListFieldType.TimetableFieldType create() {
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
        static BatchTableRequest.ToObjectReturnType0.OpListFieldType.UngroupFieldType create() {
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
        static BatchTableRequest.ToObjectReturnType0.OpListFieldType.UnstructuredfilterFieldType
            create() {
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
        static BatchTableRequest.ToObjectReturnType0.OpListFieldType.UpdateFieldType create() {
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
      static BatchTableRequest.ToObjectReturnType0.OpListFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      BatchTableRequest.ToObjectReturnType0.OpListFieldType.ComboaggregateFieldType
          getComboaggregate();

      @JsProperty
      BatchTableRequest.ToObjectReturnType0.OpListFieldType.DropcolumnsFieldType getDropcolumns();

      @JsProperty
      BatchTableRequest.ToObjectReturnType0.OpListFieldType.EmptytableFieldType getEmptytable();

      @JsProperty
      BatchTableRequest.ToObjectReturnType0.OpListFieldType.FilterFieldType getFilter();

      @JsProperty
      BatchTableRequest.ToObjectReturnType0.OpListFieldType.FlattenFieldType getFlatten();

      @JsProperty
      BatchTableRequest.ToObjectReturnType0.OpListFieldType.HeadFieldType getHead();

      @JsProperty
      BatchTableRequest.ToObjectReturnType0.OpListFieldType.HeadbyFieldType getHeadby();

      @JsProperty
      BatchTableRequest.ToObjectReturnType0.OpListFieldType.JoinFieldType getJoin();

      @JsProperty
      Object getLazyupdate();

      @JsProperty
      BatchTableRequest.ToObjectReturnType0.OpListFieldType.MergeFieldType getMerge();

      @JsProperty
      Object getSelect();

      @JsProperty
      BatchTableRequest.ToObjectReturnType0.OpListFieldType.SelectdistinctFieldType
          getSelectdistinct();

      @JsProperty
      BatchTableRequest.ToObjectReturnType0.OpListFieldType.SnapshotFieldType getSnapshot();

      @JsProperty
      BatchTableRequest.ToObjectReturnType0.OpListFieldType.SortFieldType getSort();

      @JsProperty
      Object getTail();

      @JsProperty
      Object getTailby();

      @JsProperty
      BatchTableRequest.ToObjectReturnType0.OpListFieldType.TimetableFieldType getTimetable();

      @JsProperty
      BatchTableRequest.ToObjectReturnType0.OpListFieldType.UngroupFieldType getUngroup();

      @JsProperty
      BatchTableRequest.ToObjectReturnType0.OpListFieldType.UnstructuredfilterFieldType
          getUnstructuredfilter();

      @JsProperty
      BatchTableRequest.ToObjectReturnType0.OpListFieldType.UpdateFieldType getUpdate();

      @JsProperty
      Object getUpdateview();

      @JsProperty
      Object getView();

      @JsProperty
      void setComboaggregate(
          BatchTableRequest.ToObjectReturnType0.OpListFieldType.ComboaggregateFieldType
              comboaggregate);

      @JsProperty
      void setDropcolumns(
          BatchTableRequest.ToObjectReturnType0.OpListFieldType.DropcolumnsFieldType dropcolumns);

      @JsProperty
      void setEmptytable(
          BatchTableRequest.ToObjectReturnType0.OpListFieldType.EmptytableFieldType emptytable);

      @JsProperty
      void setFilter(BatchTableRequest.ToObjectReturnType0.OpListFieldType.FilterFieldType filter);

      @JsProperty
      void setFlatten(
          BatchTableRequest.ToObjectReturnType0.OpListFieldType.FlattenFieldType flatten);

      @JsProperty
      void setHead(BatchTableRequest.ToObjectReturnType0.OpListFieldType.HeadFieldType head);

      @JsProperty
      void setHeadby(BatchTableRequest.ToObjectReturnType0.OpListFieldType.HeadbyFieldType headby);

      @JsProperty
      void setJoin(BatchTableRequest.ToObjectReturnType0.OpListFieldType.JoinFieldType join);

      @JsProperty
      void setLazyupdate(Object lazyupdate);

      @JsProperty
      void setMerge(BatchTableRequest.ToObjectReturnType0.OpListFieldType.MergeFieldType merge);

      @JsProperty
      void setSelect(Object select);

      @JsProperty
      void setSelectdistinct(
          BatchTableRequest.ToObjectReturnType0.OpListFieldType.SelectdistinctFieldType
              selectdistinct);

      @JsProperty
      void setSnapshot(
          BatchTableRequest.ToObjectReturnType0.OpListFieldType.SnapshotFieldType snapshot);

      @JsProperty
      void setSort(BatchTableRequest.ToObjectReturnType0.OpListFieldType.SortFieldType sort);

      @JsProperty
      void setTail(Object tail);

      @JsProperty
      void setTailby(Object tailby);

      @JsProperty
      void setTimetable(
          BatchTableRequest.ToObjectReturnType0.OpListFieldType.TimetableFieldType timetable);

      @JsProperty
      void setUngroup(
          BatchTableRequest.ToObjectReturnType0.OpListFieldType.UngroupFieldType ungroup);

      @JsProperty
      void setUnstructuredfilter(
          BatchTableRequest.ToObjectReturnType0.OpListFieldType.UnstructuredfilterFieldType
              unstructuredfilter);

      @JsProperty
      void setUpdate(BatchTableRequest.ToObjectReturnType0.OpListFieldType.UpdateFieldType update);

      @JsProperty
      void setUpdateview(Object updateview);

      @JsProperty
      void setView(Object view);
    }

    @JsOverlay
    static BatchTableRequest.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<BatchTableRequest.ToObjectReturnType0.OpListFieldType> getOpList();

    @JsProperty
    void setOpList(JsArray<BatchTableRequest.ToObjectReturnType0.OpListFieldType> opList);

    @JsOverlay
    default void setOpList(BatchTableRequest.ToObjectReturnType0.OpListFieldType[] opList) {
      setOpList(
          Js.<JsArray<BatchTableRequest.ToObjectReturnType0.OpListFieldType>>uncheckedCast(opList));
    }
  }

  public static native BatchTableRequest deserializeBinary(Uint8Array bytes);

  public static native BatchTableRequest deserializeBinaryFromReader(
      BatchTableRequest message, Object reader);

  public static native void serializeBinaryToWriter(BatchTableRequest message, Object writer);

  public static native BatchTableRequest.ToObjectReturnType toObject(
      boolean includeInstance, BatchTableRequest msg);

  public native Operation addOp();

  public native Operation addOp(Operation value, double index);

  public native Operation addOp(Operation value);

  public native void clearOpList();

  public native JsArray<Operation> getOpList();

  public native Uint8Array serializeBinary();

  public native void setOpList(JsArray<Operation> value);

  @JsOverlay
  public final void setOpList(Operation[] value) {
    setOpList(Js.<JsArray<Operation>>uncheckedCast(value));
  }

  public native BatchTableRequest.ToObjectReturnType0 toObject();

  public native BatchTableRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
