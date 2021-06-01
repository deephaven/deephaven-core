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
      public interface ComboAggregateFieldType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface AggregatesListFieldType {
          @JsOverlay
          static BatchTableRequest.ToObjectReturnType.OpsListFieldType.ComboAggregateFieldType
                  .AggregatesListFieldType
              create() {
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
        static BatchTableRequest.ToObjectReturnType.OpsListFieldType.ComboAggregateFieldType
            create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<
                BatchTableRequest.ToObjectReturnType.OpsListFieldType.ComboAggregateFieldType
                    .AggregatesListFieldType>
            getAggregatesList();

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
            BatchTableRequest.ToObjectReturnType.OpsListFieldType.ComboAggregateFieldType
                        .AggregatesListFieldType
                    []
                aggregatesList) {
          setAggregatesList(
              Js
                  .<JsArray<
                          BatchTableRequest.ToObjectReturnType.OpsListFieldType
                              .ComboAggregateFieldType.AggregatesListFieldType>>
                      uncheckedCast(aggregatesList));
        }

        @JsProperty
        void setAggregatesList(
            JsArray<
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.ComboAggregateFieldType
                        .AggregatesListFieldType>
                aggregatesList);

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
      public interface DropColumnsFieldType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SourceIdFieldType {
          @JsOverlay
          static BatchTableRequest.ToObjectReturnType.OpsListFieldType.DropColumnsFieldType
                  .SourceIdFieldType
              create() {
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
        BatchTableRequest.ToObjectReturnType.OpsListFieldType.DropColumnsFieldType.SourceIdFieldType
            getSourceId();

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
            BatchTableRequest.ToObjectReturnType.OpsListFieldType.DropColumnsFieldType
                    .SourceIdFieldType
                sourceId);
      }

      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface EmptyTableFieldType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultIdFieldType {
          @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
          public interface GetIdUnionType {
            @JsOverlay
            static BatchTableRequest.ToObjectReturnType.OpsListFieldType.EmptyTableFieldType
                    .ResultIdFieldType.GetIdUnionType
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
          static BatchTableRequest.ToObjectReturnType.OpsListFieldType.EmptyTableFieldType
                  .ResultIdFieldType
              create() {
            return Js.uncheckedCast(JsPropertyMap.of());
          }

          @JsProperty
          BatchTableRequest.ToObjectReturnType.OpsListFieldType.EmptyTableFieldType
                  .ResultIdFieldType.GetIdUnionType
              getId();

          @JsProperty
          void setId(
              BatchTableRequest.ToObjectReturnType.OpsListFieldType.EmptyTableFieldType
                      .ResultIdFieldType.GetIdUnionType
                  id);

          @JsOverlay
          default void setId(String id) {
            setId(
                Js
                    .<BatchTableRequest.ToObjectReturnType.OpsListFieldType.EmptyTableFieldType
                            .ResultIdFieldType.GetIdUnionType>
                        uncheckedCast(id));
          }

          @JsOverlay
          default void setId(Uint8Array id) {
            setId(
                Js
                    .<BatchTableRequest.ToObjectReturnType.OpsListFieldType.EmptyTableFieldType
                            .ResultIdFieldType.GetIdUnionType>
                        uncheckedCast(id));
          }
        }

        @JsOverlay
        static BatchTableRequest.ToObjectReturnType.OpsListFieldType.EmptyTableFieldType create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<String> getColumnNamesList();

        @JsProperty
        JsArray<String> getColumnTypesList();

        @JsProperty
        BatchTableRequest.ToObjectReturnType.OpsListFieldType.EmptyTableFieldType.ResultIdFieldType
            getResultId();

        @JsProperty
        String getSize();

        @JsProperty
        void setColumnNamesList(JsArray<String> columnNamesList);

        @JsOverlay
        default void setColumnNamesList(String[] columnNamesList) {
          setColumnNamesList(Js.<JsArray<String>>uncheckedCast(columnNamesList));
        }

        @JsProperty
        void setColumnTypesList(JsArray<String> columnTypesList);

        @JsOverlay
        default void setColumnTypesList(String[] columnTypesList) {
          setColumnTypesList(Js.<JsArray<String>>uncheckedCast(columnTypesList));
        }

        @JsProperty
        void setResultId(
            BatchTableRequest.ToObjectReturnType.OpsListFieldType.EmptyTableFieldType
                    .ResultIdFieldType
                resultId);

        @JsProperty
        void setSize(String size);
      }

      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface FilterFieldType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface FiltersListFieldType {
          @JsOverlay
          static BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType
                  .FiltersListFieldType
              create() {
            return Js.uncheckedCast(JsPropertyMap.of());
          }

          @JsProperty
          JsArray<Object> getChildrenList();

          @JsProperty
          double getDoubleValue();

          @JsProperty
          double getFilterOp();

          @JsProperty
          double getLongValue();

          @JsProperty
          double getNanoTimeValue();

          @JsProperty
          String getStringValue();

          @JsProperty
          boolean isBoolValue();

          @JsProperty
          void setBoolValue(boolean boolValue);

          @JsProperty
          void setChildrenList(JsArray<Object> childrenList);

          @JsOverlay
          default void setChildrenList(Object[] childrenList) {
            setChildrenList(Js.<JsArray<Object>>uncheckedCast(childrenList));
          }

          @JsProperty
          void setDoubleValue(double doubleValue);

          @JsProperty
          void setFilterOp(double filterOp);

          @JsProperty
          void setLongValue(double longValue);

          @JsProperty
          void setNanoTimeValue(double nanoTimeValue);

          @JsProperty
          void setStringValue(String stringValue);
        }

        @JsOverlay
        static BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<
                BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType
                    .FiltersListFieldType>
            getFiltersList();

        @JsProperty
        Object getResultId();

        @JsProperty
        Object getSourceId();

        @JsOverlay
        default void setFiltersList(
            BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType
                        .FiltersListFieldType
                    []
                filtersList) {
          setFiltersList(
              Js
                  .<JsArray<
                          BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType
                              .FiltersListFieldType>>
                      uncheckedCast(filtersList));
        }

        @JsProperty
        void setFiltersList(
            JsArray<
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType
                        .FiltersListFieldType>
                filtersList);

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
      public interface JoinFieldType {
        @JsOverlay
        static BatchTableRequest.ToObjectReturnType.OpsListFieldType.JoinFieldType create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<String> getColumnsToAddList();

        @JsProperty
        JsArray<String> getColumnsToMatchList();

        @JsProperty
        double getJoinType();

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
        void setJoinType(double joinType);

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
      public interface SelectDistinctFieldType {
        @JsOverlay
        static BatchTableRequest.ToObjectReturnType.OpsListFieldType.SelectDistinctFieldType
            create() {
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
          static BatchTableRequest.ToObjectReturnType.OpsListFieldType.SortFieldType
                  .SortsListFieldType
              create() {
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
        JsArray<
                BatchTableRequest.ToObjectReturnType.OpsListFieldType.SortFieldType
                    .SortsListFieldType>
            getSortsList();

        @JsProperty
        Object getSourceId();

        @JsProperty
        void setResultId(Object resultId);

        @JsProperty
        void setSortsList(
            JsArray<
                    BatchTableRequest.ToObjectReturnType.OpsListFieldType.SortFieldType
                        .SortsListFieldType>
                sortsList);

        @JsOverlay
        default void setSortsList(
            BatchTableRequest.ToObjectReturnType.OpsListFieldType.SortFieldType.SortsListFieldType[]
                sortsList) {
          setSortsList(
              Js
                  .<JsArray<
                          BatchTableRequest.ToObjectReturnType.OpsListFieldType.SortFieldType
                              .SortsListFieldType>>
                      uncheckedCast(sortsList));
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
        static BatchTableRequest.ToObjectReturnType.OpsListFieldType.UnstructuredFilterFieldType
            create() {
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
      BatchTableRequest.ToObjectReturnType.OpsListFieldType.ComboAggregateFieldType
          getComboAggregate();

      @JsProperty
      BatchTableRequest.ToObjectReturnType.OpsListFieldType.DropColumnsFieldType getDropColumns();

      @JsProperty
      BatchTableRequest.ToObjectReturnType.OpsListFieldType.EmptyTableFieldType getEmptyTable();

      @JsProperty
      BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType getFilter();

      @JsProperty
      BatchTableRequest.ToObjectReturnType.OpsListFieldType.FlattenFieldType getFlatten();

      @JsProperty
      BatchTableRequest.ToObjectReturnType.OpsListFieldType.HeadFieldType getHead();

      @JsProperty
      BatchTableRequest.ToObjectReturnType.OpsListFieldType.HeadByFieldType getHeadBy();

      @JsProperty
      BatchTableRequest.ToObjectReturnType.OpsListFieldType.JoinFieldType getJoin();

      @JsProperty
      Object getLazyUpdate();

      @JsProperty
      BatchTableRequest.ToObjectReturnType.OpsListFieldType.MergeFieldType getMerge();

      @JsProperty
      Object getSelect();

      @JsProperty
      BatchTableRequest.ToObjectReturnType.OpsListFieldType.SelectDistinctFieldType
          getSelectDistinct();

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
      BatchTableRequest.ToObjectReturnType.OpsListFieldType.UnstructuredFilterFieldType
          getUnstructuredFilter();

      @JsProperty
      BatchTableRequest.ToObjectReturnType.OpsListFieldType.UpdateFieldType getUpdate();

      @JsProperty
      Object getUpdateView();

      @JsProperty
      Object getView();

      @JsProperty
      void setComboAggregate(
          BatchTableRequest.ToObjectReturnType.OpsListFieldType.ComboAggregateFieldType
              comboAggregate);

      @JsProperty
      void setDropColumns(
          BatchTableRequest.ToObjectReturnType.OpsListFieldType.DropColumnsFieldType dropColumns);

      @JsProperty
      void setEmptyTable(
          BatchTableRequest.ToObjectReturnType.OpsListFieldType.EmptyTableFieldType emptyTable);

      @JsProperty
      void setFilter(BatchTableRequest.ToObjectReturnType.OpsListFieldType.FilterFieldType filter);

      @JsProperty
      void setFlatten(
          BatchTableRequest.ToObjectReturnType.OpsListFieldType.FlattenFieldType flatten);

      @JsProperty
      void setHead(BatchTableRequest.ToObjectReturnType.OpsListFieldType.HeadFieldType head);

      @JsProperty
      void setHeadBy(BatchTableRequest.ToObjectReturnType.OpsListFieldType.HeadByFieldType headBy);

      @JsProperty
      void setJoin(BatchTableRequest.ToObjectReturnType.OpsListFieldType.JoinFieldType join);

      @JsProperty
      void setLazyUpdate(Object lazyUpdate);

      @JsProperty
      void setMerge(BatchTableRequest.ToObjectReturnType.OpsListFieldType.MergeFieldType merge);

      @JsProperty
      void setSelect(Object select);

      @JsProperty
      void setSelectDistinct(
          BatchTableRequest.ToObjectReturnType.OpsListFieldType.SelectDistinctFieldType
              selectDistinct);

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
          BatchTableRequest.ToObjectReturnType.OpsListFieldType.UnstructuredFilterFieldType
              unstructuredFilter);

      @JsProperty
      void setUpdate(BatchTableRequest.ToObjectReturnType.OpsListFieldType.UpdateFieldType update);

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
      public interface ComboAggregateFieldType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface AggregatesListFieldType {
          @JsOverlay
          static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.ComboAggregateFieldType
                  .AggregatesListFieldType
              create() {
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
        static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.ComboAggregateFieldType
            create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<
                BatchTableRequest.ToObjectReturnType0.OpsListFieldType.ComboAggregateFieldType
                    .AggregatesListFieldType>
            getAggregatesList();

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
            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.ComboAggregateFieldType
                        .AggregatesListFieldType
                    []
                aggregatesList) {
          setAggregatesList(
              Js
                  .<JsArray<
                          BatchTableRequest.ToObjectReturnType0.OpsListFieldType
                              .ComboAggregateFieldType.AggregatesListFieldType>>
                      uncheckedCast(aggregatesList));
        }

        @JsProperty
        void setAggregatesList(
            JsArray<
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.ComboAggregateFieldType
                        .AggregatesListFieldType>
                aggregatesList);

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
      public interface DropColumnsFieldType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SourceIdFieldType {
          @JsOverlay
          static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.DropColumnsFieldType
                  .SourceIdFieldType
              create() {
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
        static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.DropColumnsFieldType
            create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<String> getColumnNamesList();

        @JsProperty
        Object getResultId();

        @JsProperty
        BatchTableRequest.ToObjectReturnType0.OpsListFieldType.DropColumnsFieldType
                .SourceIdFieldType
            getSourceId();

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
            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.DropColumnsFieldType
                    .SourceIdFieldType
                sourceId);
      }

      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface EmptyTableFieldType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultIdFieldType {
          @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
          public interface GetIdUnionType {
            @JsOverlay
            static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.EmptyTableFieldType
                    .ResultIdFieldType.GetIdUnionType
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
          static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.EmptyTableFieldType
                  .ResultIdFieldType
              create() {
            return Js.uncheckedCast(JsPropertyMap.of());
          }

          @JsProperty
          BatchTableRequest.ToObjectReturnType0.OpsListFieldType.EmptyTableFieldType
                  .ResultIdFieldType.GetIdUnionType
              getId();

          @JsProperty
          void setId(
              BatchTableRequest.ToObjectReturnType0.OpsListFieldType.EmptyTableFieldType
                      .ResultIdFieldType.GetIdUnionType
                  id);

          @JsOverlay
          default void setId(String id) {
            setId(
                Js
                    .<BatchTableRequest.ToObjectReturnType0.OpsListFieldType.EmptyTableFieldType
                            .ResultIdFieldType.GetIdUnionType>
                        uncheckedCast(id));
          }

          @JsOverlay
          default void setId(Uint8Array id) {
            setId(
                Js
                    .<BatchTableRequest.ToObjectReturnType0.OpsListFieldType.EmptyTableFieldType
                            .ResultIdFieldType.GetIdUnionType>
                        uncheckedCast(id));
          }
        }

        @JsOverlay
        static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.EmptyTableFieldType create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<String> getColumnNamesList();

        @JsProperty
        JsArray<String> getColumnTypesList();

        @JsProperty
        BatchTableRequest.ToObjectReturnType0.OpsListFieldType.EmptyTableFieldType.ResultIdFieldType
            getResultId();

        @JsProperty
        String getSize();

        @JsProperty
        void setColumnNamesList(JsArray<String> columnNamesList);

        @JsOverlay
        default void setColumnNamesList(String[] columnNamesList) {
          setColumnNamesList(Js.<JsArray<String>>uncheckedCast(columnNamesList));
        }

        @JsProperty
        void setColumnTypesList(JsArray<String> columnTypesList);

        @JsOverlay
        default void setColumnTypesList(String[] columnTypesList) {
          setColumnTypesList(Js.<JsArray<String>>uncheckedCast(columnTypesList));
        }

        @JsProperty
        void setResultId(
            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.EmptyTableFieldType
                    .ResultIdFieldType
                resultId);

        @JsProperty
        void setSize(String size);
      }

      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface FilterFieldType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface FiltersListFieldType {
          @JsOverlay
          static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType
                  .FiltersListFieldType
              create() {
            return Js.uncheckedCast(JsPropertyMap.of());
          }

          @JsProperty
          JsArray<Object> getChildrenList();

          @JsProperty
          double getDoubleValue();

          @JsProperty
          double getFilterOp();

          @JsProperty
          double getLongValue();

          @JsProperty
          double getNanoTimeValue();

          @JsProperty
          String getStringValue();

          @JsProperty
          boolean isBoolValue();

          @JsProperty
          void setBoolValue(boolean boolValue);

          @JsProperty
          void setChildrenList(JsArray<Object> childrenList);

          @JsOverlay
          default void setChildrenList(Object[] childrenList) {
            setChildrenList(Js.<JsArray<Object>>uncheckedCast(childrenList));
          }

          @JsProperty
          void setDoubleValue(double doubleValue);

          @JsProperty
          void setFilterOp(double filterOp);

          @JsProperty
          void setLongValue(double longValue);

          @JsProperty
          void setNanoTimeValue(double nanoTimeValue);

          @JsProperty
          void setStringValue(String stringValue);
        }

        @JsOverlay
        static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<
                BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType
                    .FiltersListFieldType>
            getFiltersList();

        @JsProperty
        Object getResultId();

        @JsProperty
        Object getSourceId();

        @JsOverlay
        default void setFiltersList(
            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType
                        .FiltersListFieldType
                    []
                filtersList) {
          setFiltersList(
              Js
                  .<JsArray<
                          BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType
                              .FiltersListFieldType>>
                      uncheckedCast(filtersList));
        }

        @JsProperty
        void setFiltersList(
            JsArray<
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType
                        .FiltersListFieldType>
                filtersList);

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
      public interface JoinFieldType {
        @JsOverlay
        static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.JoinFieldType create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<String> getColumnsToAddList();

        @JsProperty
        JsArray<String> getColumnsToMatchList();

        @JsProperty
        double getJoinType();

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
        void setJoinType(double joinType);

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
      public interface SelectDistinctFieldType {
        @JsOverlay
        static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.SelectDistinctFieldType
            create() {
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
          static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.SortFieldType
                  .SortsListFieldType
              create() {
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
        JsArray<
                BatchTableRequest.ToObjectReturnType0.OpsListFieldType.SortFieldType
                    .SortsListFieldType>
            getSortsList();

        @JsProperty
        Object getSourceId();

        @JsProperty
        void setResultId(Object resultId);

        @JsProperty
        void setSortsList(
            JsArray<
                    BatchTableRequest.ToObjectReturnType0.OpsListFieldType.SortFieldType
                        .SortsListFieldType>
                sortsList);

        @JsOverlay
        default void setSortsList(
            BatchTableRequest.ToObjectReturnType0.OpsListFieldType.SortFieldType.SortsListFieldType
                    []
                sortsList) {
          setSortsList(
              Js
                  .<JsArray<
                          BatchTableRequest.ToObjectReturnType0.OpsListFieldType.SortFieldType
                              .SortsListFieldType>>
                      uncheckedCast(sortsList));
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
        static BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UnstructuredFilterFieldType
            create() {
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
      BatchTableRequest.ToObjectReturnType0.OpsListFieldType.ComboAggregateFieldType
          getComboAggregate();

      @JsProperty
      BatchTableRequest.ToObjectReturnType0.OpsListFieldType.DropColumnsFieldType getDropColumns();

      @JsProperty
      BatchTableRequest.ToObjectReturnType0.OpsListFieldType.EmptyTableFieldType getEmptyTable();

      @JsProperty
      BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType getFilter();

      @JsProperty
      BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FlattenFieldType getFlatten();

      @JsProperty
      BatchTableRequest.ToObjectReturnType0.OpsListFieldType.HeadFieldType getHead();

      @JsProperty
      BatchTableRequest.ToObjectReturnType0.OpsListFieldType.HeadByFieldType getHeadBy();

      @JsProperty
      BatchTableRequest.ToObjectReturnType0.OpsListFieldType.JoinFieldType getJoin();

      @JsProperty
      Object getLazyUpdate();

      @JsProperty
      BatchTableRequest.ToObjectReturnType0.OpsListFieldType.MergeFieldType getMerge();

      @JsProperty
      Object getSelect();

      @JsProperty
      BatchTableRequest.ToObjectReturnType0.OpsListFieldType.SelectDistinctFieldType
          getSelectDistinct();

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
      BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UnstructuredFilterFieldType
          getUnstructuredFilter();

      @JsProperty
      BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UpdateFieldType getUpdate();

      @JsProperty
      Object getUpdateView();

      @JsProperty
      Object getView();

      @JsProperty
      void setComboAggregate(
          BatchTableRequest.ToObjectReturnType0.OpsListFieldType.ComboAggregateFieldType
              comboAggregate);

      @JsProperty
      void setDropColumns(
          BatchTableRequest.ToObjectReturnType0.OpsListFieldType.DropColumnsFieldType dropColumns);

      @JsProperty
      void setEmptyTable(
          BatchTableRequest.ToObjectReturnType0.OpsListFieldType.EmptyTableFieldType emptyTable);

      @JsProperty
      void setFilter(BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FilterFieldType filter);

      @JsProperty
      void setFlatten(
          BatchTableRequest.ToObjectReturnType0.OpsListFieldType.FlattenFieldType flatten);

      @JsProperty
      void setHead(BatchTableRequest.ToObjectReturnType0.OpsListFieldType.HeadFieldType head);

      @JsProperty
      void setHeadBy(BatchTableRequest.ToObjectReturnType0.OpsListFieldType.HeadByFieldType headBy);

      @JsProperty
      void setJoin(BatchTableRequest.ToObjectReturnType0.OpsListFieldType.JoinFieldType join);

      @JsProperty
      void setLazyUpdate(Object lazyUpdate);

      @JsProperty
      void setMerge(BatchTableRequest.ToObjectReturnType0.OpsListFieldType.MergeFieldType merge);

      @JsProperty
      void setSelect(Object select);

      @JsProperty
      void setSelectDistinct(
          BatchTableRequest.ToObjectReturnType0.OpsListFieldType.SelectDistinctFieldType
              selectDistinct);

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
          BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UnstructuredFilterFieldType
              unstructuredFilter);

      @JsProperty
      void setUpdate(BatchTableRequest.ToObjectReturnType0.OpsListFieldType.UpdateFieldType update);

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
