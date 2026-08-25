//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import elemental2.core.ReadonlyArray;
import elemental2.promise.Promise;
import io.deephaven.proto.backplane.grpc.AggSpec;
import io.deephaven.proto.backplane.grpc.AggregateAllRequest;
import io.deephaven.proto.backplane.grpc.AggregateRequest;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.DropColumnsRequest;
import io.deephaven.proto.backplane.grpc.FilterTableRequest;
import io.deephaven.proto.backplane.grpc.FlattenRequest;
import io.deephaven.proto.backplane.grpc.HeadOrTailRequest;
import io.deephaven.proto.backplane.grpc.SelectOrUpdateRequest;
import io.deephaven.proto.backplane.grpc.SliceRequest;
import io.deephaven.proto.backplane.grpc.SnapshotTableRequest;
import io.deephaven.proto.backplane.grpc.SnapshotWhenTableRequest;
import io.deephaven.proto.backplane.grpc.SortDescriptor;
import io.deephaven.proto.backplane.grpc.SortTableRequest;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.backplane.grpc.UngroupRequest;
import io.deephaven.proto.backplane.grpc.WhereInRequest;
import io.deephaven.web.client.api.agg.*;
import io.deephaven.web.client.api.filter.FilterCondition;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsOptional;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Describes operations that can be performed on a table without retrieving data or metadata, allowing these operations
 * to potentially be chained with no direct interaction with a promise.
 * <p>
 *
 * <pre>
 * async function process(table: dh.TableOperations) {
 *   // llm generated strawman syntax for where/sort
 *   const result = await table
 *       .where([{column: "A", op: ">", value: 5}])
 *       .sort([{column: "B", direction: "DESC"}])
 *       .head(10);
 *   // Any use of methods on table after this point would be potentially invalid, we didn't retain it ourselves,
 *   // and don't know if the caller did either.
 *   try {
 *       const a = result.findColumn('A');
 *       const data = await result.createSnapshot({
 *           rows:{first:0, last:10},
 *           columns:[a, 'B']
 *       });
 *       return data.get(0, a);
 *   } finally {
 *       result.close();
 *   }
 * }
 * </pre>
 * 
 * The caller here might be passing in an existing ResolvedTable so later operations are safe, but it isn't declared
 * that way, so we can't be sure, and the method shouldn't rely on it. Here's an example instead of retaining a table
 * for later reuse - both the initial table is retained upon creation, then each change in filter is retained as well.
 * Each retained table must be released when being replaced, or when the entire instance will no longer be used.
 * 
 * <pre>
 * class SwapFilters() {
 *     constructor(table: dh.TableOperations) {
 *         this.filters: ReadonlyArray&lt;dh.FilterCondition> = [];
 *         this.table: Promise&lt;dh.ResolvedTable> = table.resolve();
 *         this.filteredTable: null|Promise&lt;dh.ResolvedTable> = null;
 *     }
 *     changeFilters(filters: ReadonlyArray&lt;dh.FilterCondition>) {
 *         if (filteredTable) {
 *             filteredTable.then(t => t.close());
 *         }
 *         this.filters = filters;
 *         // Note that this can be a surprise - returning a Thenable or PromiseLike as the result
 *         // of a resolved callback will invoke `then()` on it, which will retain the table.
 *         // In this case, this is what we want, but it could still be a surprise in some cases.
 *         this.filteredTable = this.table.then(t => t.where(filters));
 *     }
 *     async loadData():dh.TableData {
 *         if (!this.filteredTable) {
 *             throw new Error("no filters set");
 *         }
 *         const t = await this.filteredTable!;
 *         return t.createSnapshot({
 *             rows: {first:0, last:1_000_000},
 *             columns: t.columns
 *         })
 *     }
 *     close() {
 *         this.table.then(t => t.close());
 *         if (this.filteredTable) {
 *             this.filteredTable.then(t => t.close());
 *         }
 *     }
 * }
 * </pre>
 */
@TsName(namespace = "dh", name = "TableOperations")
@TsInterface
public interface JsTableOperations extends ServerObject {

    /**
     * When unsure if this is a {@link JsPendingTable} or a {@link JsResolvedTable}, this will unambiguously provide a
     * promise that results in a {@link JsResolvedTable}. Alternatively, one could simply await this instance and assume
     * the result is a ResolvedTable, but this is a type-safe alternative to that.
     *
     * @return a promise that resolves to a retained ResolvedTable instance
     */
    @JsMethod
    Promise<JsResolvedTable> resolve();

    /**
     * Internal method to make the async call to the server.
     *
     * @param resultId the ticket that the server will populate with this result
     * @param operation the operation to perform
     * @return a table operations instance that more calls can be chained on, or can be awaited
     */
    JsPendingTable call(Ticket resultId, BatchTableRequest.Operation.Builder operation);

    @JsMethod
    default JsPendingTable head(TableData.RowPositionUnion size) {
        Ticket ticket = getConnection().getTickets().newExportTicket();

        return call(ticket, BatchTableRequest.Operation.newBuilder()
                .setHead(HeadOrTailRequest.newBuilder()
                        .setSourceId(TableReference.newBuilder()
                                .setTicket(typedTicket().getTicket())
                                .build())
                        .setResultId(ticket)
                        .setNumRows(size.asInt())));
    }

    @JsMethod
    default JsPendingTable tail(TableData.RowPositionUnion size) {
        Ticket ticket = getConnection().getTickets().newExportTicket();

        return call(ticket, BatchTableRequest.Operation.newBuilder()
                .setTail(HeadOrTailRequest.newBuilder()
                        .setSourceId(TableReference.newBuilder()
                                .setTicket(typedTicket().getTicket())
                                .build())
                        .setResultId(ticket)
                        .setNumRows(size.asInt())));
    }

    @JsMethod
    default JsPendingTable reverse() {
        Ticket ticket = getConnection().getTickets().newExportTicket();

        return call(ticket, BatchTableRequest.Operation.newBuilder()
                .setSort(SortTableRequest.newBuilder()
                        .setSourceId(TableReference.newBuilder()
                                .setTicket(typedTicket().getTicket())
                                .build())
                        .setResultId(ticket)
                        .addSorts(SortDescriptor.newBuilder()
                                .setDirection(SortDescriptor.SortDirection.REVERSE))));
    }

    @JsMethod
    default JsPendingTable snapshot() {
        Ticket ticket = getConnection().getTickets().newExportTicket();

        return call(ticket, BatchTableRequest.Operation.newBuilder().setSnapshot(SnapshotTableRequest.newBuilder()
                .setSourceId(TableReference.newBuilder()
                        .setTicket(typedTicket().getTicket())
                        .build())
                .setResultId(ticket)));
    }

    @TsInterface
    @JsType(namespace = "dh")
    class SnapshotWhenOptions {
        /**
         * Whether to take an initial snapshot of the base table upon creation. If false, the resulting table will
         * remain empty until the trigger table first updates. Defaults to false.
         */
        @JsProperty
        @JsNullable
        public Boolean initial;

        /**
         * Whether the resulting table should be incremental. When incremental, only the rows of the base table that
         * have been added or updated will have the latest "stamp key". Defaults to false.
         */
        @JsProperty
        @JsNullable
        public Boolean incremental;

        /**
         * Whether the resulting table should keep history. A history table appends a full snapshot of the starting
         * table and the "stamp key" as opposed to updating existing rows. When this flag is set, the trigger table must
         * be append-only
         */
        @JsProperty
        @JsNullable
        public Boolean history;

        /**
         * One or more column names to act as stamp columns. Each stamp column will be included in the final result, and
         * will contain the value of the stamp column from the trigger table at the time of the snapshot. If empty or
         * not specified, all columns will be used.
         */
        @JsProperty
        @JsNullable
        public ReadonlyArray<String> stampColumns;
    }

    /**
     * Creates a table that captures a snapshot of this table whenever the trigger table updates.
     *
     * @param trigger the table to use as a trigger for when to take a snapshot of this table
     * @param options options on how the result table should be updated
     * @return the snapshot table
     */
    @JsMethod
    default JsPendingTable snapshotWhen(JsTableOperations trigger, SnapshotWhenOptions options) {
        Ticket ticket = getConnection().getTickets().newExportTicket();

        return call(ticket, BatchTableRequest.Operation.newBuilder().setSnapshotWhen(SnapshotWhenTableRequest
                .newBuilder()
                .setBaseId(TableReference.newBuilder()
                        .setTicket(typedTicket().getTicket())
                        .build())
                .setTriggerId(TableReference.newBuilder()
                        .setTicket(trigger.typedTicket().getTicket())
                        .build())
                .setResultId(ticket)
                .setInitial(options.initial != null && options.initial)
                .setIncremental(options.incremental != null && options.incremental)
                .setHistory(options.history != null && options.history)
                .addAllStampColumns(
                        options.stampColumns != null ? options.stampColumns.asList() : Collections.emptyList())));
    }

    // omitting sortDescending, sort objects have this for us
    @JsMethod
    default JsPendingTable sort(ReadonlyArray<Sort.SortUnion> sorts) {
        Ticket ticket = getConnection().getTickets().newExportTicket();

        return call(ticket, BatchTableRequest.Operation.newBuilder().setSort(SortTableRequest.newBuilder()
                .setSourceId(TableReference.newBuilder()
                        .setTicket(typedTicket().getTicket())
                        .build())
                .setResultId(ticket)
                .addAllSorts(sorts.asList().stream().map(Sort.SortUnion::makeDescriptor).toList())));
    }

    /**
     * Creates a table that filters the contents of this table based on the provided conditions.
     *
     * @param conditions the conditions to filter on
     * @return a filtered table
     */
    // TODO consider a simple struct that has the same shape, allows not accessing columns
    @JsMethod
    default JsPendingTable where(ReadonlyArray<FilterCondition> conditions) {
        Ticket ticket = getConnection().getTickets().newExportTicket();

        return call(ticket, BatchTableRequest.Operation.newBuilder().setFilter(FilterTableRequest.newBuilder()
                .setSourceId(TableReference.newBuilder()
                        .setTicket(typedTicket().getTicket())
                        .build())
                .setResultId(ticket)
                .addAllFilters(conditions.asList().stream().map(FilterCondition::makeDescriptor).toList())));
    }

    /**
     * Filters the contents of the left table (e.g. this table) based on the set of values that match the provided right
     * table.
     * <p>
     * Note that when the right table ticks, all the rows in the left table will be re-evaluated.
     *
     * @param rightTable the table with the filter criteria
     * @param columnsToMatch the column matches between the two tables
     * @return a new filtered table
     */
    @JsMethod
    default JsPendingTable whereIn(JsTableOperations rightTable, ReadonlyArray<Column.ColumnOrName> columnsToMatch) {
        Ticket ticket = getConnection().getTickets().newExportTicket();

        return call(ticket, BatchTableRequest.Operation.newBuilder().setWhereIn(WhereInRequest.newBuilder()
                .setResultId(ticket)
                .setLeftId(TableReference.newBuilder()
                        .setTicket(typedTicket().getTicket()))
                .setRightId(TableReference.newBuilder()
                        .setTicket(rightTable.typedTicket().getTicket()))
                .addAllColumnsToMatch(columnsToNameList(columnsToMatch))
                .setInverted(false)
                .build()));
    }

    /**
     * Filters the contents of the left table (this table) based on the set of values that do not match the provided
     * right table.
     * <p>
     * Note that when the right table ticks, all the rows in the left table will be re-evaluated.
     *
     * @param rightTable the table with the filter criteria
     * @param columnsToMatch the column matches between the two tables
     * @return a new filtered table
     */
    @JsMethod
    default JsPendingTable whereNotIn(JsTableOperations rightTable, ReadonlyArray<Column.ColumnOrName> columnsToMatch) {
        Ticket ticket = getConnection().getTickets().newExportTicket();

        return call(ticket, BatchTableRequest.Operation.newBuilder().setWhereIn(WhereInRequest.newBuilder()
                .setResultId(ticket)
                .setLeftId(TableReference.newBuilder()
                        .setTicket(typedTicket().getTicket()))
                .setRightId(TableReference.newBuilder()
                        .setTicket(rightTable.typedTicket().getTicket()))
                .addAllColumnsToMatch(columnsToNameList(columnsToMatch))
                .setInverted(true)
                .build()));
    }

    /**
     * Extracts a subset of a table by row position.
     * <p>
     * If both firstPosition and lastPosition are positive, then the rows are counted from the beginning of the table.
     * The firstPosition is inclusive, and the lastPosition is exclusive. The {@link #head}(N) call is equivalent to
     * slice(0, N). The firstPosition must be less than or equal to the lastPosition.
     * <p>
     * If firstPosition is positive and lastPosition is negative, then the firstRow is counted from the beginning of the
     * table, inclusively. The lastPosition is counted from the end of the table. For example, slice(1, -1) includes all
     * rows but the first and last. If the lastPosition would be before the firstRow, the result is an emptyTable.
     * <p>
     * If firstPosition is negative, and lastPosition is zero, then the firstRow is counted from the end of the table,
     * and the end of the slice is the size of the table. slice(-N, 0) is equivalent to {@link #tail}(N).
     * <p>
     * If the firstPosition is negative and the lastPosition is negative, they are both counted from the end of the
     * table. For example, slice(-2, -1) returns the second to last row of the table.
     * <p>
     * If firstPosition is negative and lastPosition is positive, then firstPosition is counted from the end of the
     * table, inclusively. The lastPosition is counted from the beginning of the table, exclusively. For example,
     * slice(-3, 5) returns all rows starting from the third-last row to the fifth row of the table. If there are no
     * rows between these positions, the function will return an empty table.
     *
     * @param firstPositionInclusive the first position to include in the result
     * @param lastPositionExclusive tthe last position to include in the result
     * @return a new table with the subset of rows from the original table
     */
    @JsMethod
    default JsPendingTable slice(TableData.RowPositionUnion firstPositionInclusive,
            TableData.RowPositionUnion lastPositionExclusive) {
        Ticket ticket = getConnection().getTickets().newExportTicket();

        return call(ticket, BatchTableRequest.Operation.newBuilder().setSlice(SliceRequest.newBuilder()
                .setResultId(ticket)
                .setSourceId(TableReference.newBuilder()
                        .setTicket(typedTicket().getTicket())
                        .build())
                .setFirstPositionInclusive(firstPositionInclusive.getLongValue())
                .setLastPositionExclusive(lastPositionExclusive.getLongValue())
                .build()));
    }

    private static SelectOrUpdateRequest.Builder selectOrUpdate(
            ReadonlyArray<JsTable.CustomColumnArgUnionType> columns) {
        return SelectOrUpdateRequest.newBuilder()
                .addAllColumnSpecs(columns.asList().stream()
                        .map(JsTable.CustomColumnArgUnionType::makeColumnSpec)
                        .collect(Collectors.toList()));
    }

    /**
     * Creates a new table with only the specified columns. Any formulas will be computed each time the value is
     * requested, nothing will be stored.
     *
     * @param columns the columns or expressions to have in the new table
     * @return a new table with only those columns.
     */
    @JsMethod
    default JsPendingTable view(ReadonlyArray<JsTable.CustomColumnArgUnionType> columns) {
        Ticket ticket = getConnection().getTickets().newExportTicket();

        return call(ticket, BatchTableRequest.Operation.newBuilder().setView(selectOrUpdate(columns)
                .setSourceId(TableReference.newBuilder()
                        .setTicket(typedTicket().getTicket())
                        .build())
                .setResultId(ticket)));
    }

    /**
     * Creates a new table, appending or replacing the specified columns. Any formulas will be computed each time the
     * value is requested, nothing will be stored.
     *
     * @param columns the columns or expressions to have in the new table
     * @return a new table with those columns appended
     */
    @JsMethod
    default JsPendingTable updateView(ReadonlyArray<JsTable.CustomColumnArgUnionType> columns) {
        Ticket ticket = getConnection().getTickets().newExportTicket();

        return call(ticket, BatchTableRequest.Operation.newBuilder().setUpdateView(selectOrUpdate(columns)
                .setSourceId(TableReference.newBuilder()
                        .setTicket(typedTicket().getTicket())
                        .build())
                .setResultId(ticket)));
    }

    /**
     * Creates a new table, appending or replacing the specified columns. All formulas will be computed eagerly and
     * their values stored in memory.
     *
     * @param columns the columns or expressions to have in the new table
     * @return a new table with those columns appended
     */
    @JsMethod
    default JsPendingTable update(ReadonlyArray<JsTable.CustomColumnArgUnionType> columns) {
        Ticket ticket = getConnection().getTickets().newExportTicket();

        return call(ticket, BatchTableRequest.Operation.newBuilder().setUpdate(selectOrUpdate(columns)
                .setSourceId(TableReference.newBuilder()
                        .setTicket(typedTicket().getTicket())
                        .build())
                .setResultId(ticket)));
    }

    /**
     * Creates a new table appending or replacing the specified columns. All formulas will be computed the first time
     * they are referenced and their values stored in memory.
     *
     * @param columns the columns or expressions to have in the new table
     * @return a new table with those columns appended
     */
    @JsMethod
    default JsPendingTable lazyUpdate(ReadonlyArray<JsTable.CustomColumnArgUnionType> columns) {
        Ticket ticket = getConnection().getTickets().newExportTicket();

        return call(ticket, BatchTableRequest.Operation.newBuilder().setLazyUpdate(selectOrUpdate(columns)
                .setSourceId(TableReference.newBuilder()
                        .setTicket(typedTicket().getTicket())
                        .build())
                .setResultId(ticket)));
    }

    /**
     * Creates a new table with only the specified columns. All formulas will be computed eagerly and their values
     * stored in memory.
     *
     * @param columns the columns or expressions to have in the new table
     * @return a new table with only those columns.
     */
    @JsMethod
    default JsPendingTable select(ReadonlyArray<JsTable.CustomColumnArgUnionType> columns) {
        Ticket ticket = getConnection().getTickets().newExportTicket();

        return call(ticket, BatchTableRequest.Operation.newBuilder().setSelect(selectOrUpdate(columns)
                .setSourceId(TableReference.newBuilder()
                        .setTicket(typedTicket().getTicket())
                        .build())
                .setResultId(ticket)));
    }

    // TODO add an options type for these various flags?
    // @JsMethod
    // JsTableOperations naturalJoin(JsTableOperations rightTable, JsArray<String> columnsToMatch,
    // @JsOptional @JsNullable JsArray<String> columnsToAdd, String type);
    //
    // @JsMethod
    // JsTableOperations exactJoin(JsTableOperations rightTable, JsArray<String> columnsToMatch,
    // JsArray<String> columnsToAdd);
    //
    // @JsMethod
    // JsTableOperations join(JsTableOperations rightTable, JsArray<String> columnsToMatch, JsArray<String>
    // columnsToAdd,
    // int reserveBits);
    //
    // @JsMethod
    // JsTableOperations asOfJoin(JsTableOperations rightTable, JsArray<String> columnsToMatch,
    // @JsOptional @JsNullable JsArray<String> columnsToAdd, @JsOptional @JsNullable String asOfMatchRule);

    // TODO add args in a js-ish way
    // @JsMethod
    // JsTableOperations rangeJoin(JsTableOperations rightTable,

    /**
     * Groups the table by the specified columns, accumulating the other columns into arrays. If no columns are
     * provided, the resulting table will have a single row.
     * 
     * @param groupByColumns columns to group
     * @return a table with one row per group
     */
    @JsMethod
    default JsTableOperations groupBy(@JsOptional @JsNullable ReadonlyArray<Column.ColumnOrName> groupByColumns) {
        Ticket ticket = getConnection().getTickets().newExportTicket();

        return call(ticket, BatchTableRequest.Operation.newBuilder().setAggregateAll(AggregateAllRequest.newBuilder()
                .setSourceId(TableReference.newBuilder()
                        .setTicket(typedTicket().getTicket())
                        .build())
                .setResultId(ticket)
                .addAllGroupByColumns(
                        groupByColumns != null ? columnsToNameList(groupByColumns) : Collections.emptyList())));
    }

    /**
     * Applies a single aggregation to all columns.
     * 
     * @param aggUnion the aggregation to apply
     * @param groupByColumns columns to group by
     * @return a new table with these aggregations applied to the data in this table
     */
    @JsMethod
    default JsTableOperations aggAllBy(AggAllByUnion aggUnion,
            @JsOptional @JsNullable ReadonlyArray<Column.ColumnOrName> groupByColumns) {
        Ticket ticket = getConnection().getTickets().newExportTicket();

        AggSpec.Builder spec = aggUnion.makeAggSpec();
        return call(ticket, BatchTableRequest.Operation.newBuilder().setAggregateAll(AggregateAllRequest.newBuilder()
                .setResultId(ticket)
                .setSourceId(TableReference.newBuilder()
                        .setTicket(typedTicket().getTicket())
                        .build())
                .setSpec(spec)
                .addAllGroupByColumns(
                        groupByColumns != null ? columnsToNameList(groupByColumns) : Collections.emptyList())));
    }

    /**
     * Aggregates the contents of this table into a new table.
     * 
     * @param options
     * @return
     */
    @JsMethod
    default JsTableOperations aggBy(AggByOptions options) {
        Ticket ticket = getConnection().getTickets().newExportTicket();

        AggregateRequest.Builder aggBuilder = AggregateRequest.newBuilder()
                .setResultId(ticket)
                .setSourceId(TableReference.newBuilder()
                        .setTicket(typedTicket().getTicket())
                        .build())
                .setPreserveEmpty(options.preserveEmpty != null && options.preserveEmpty);

        for (int i = 0; i < options.aggregations.getLength(); i++) {
            AggregationUnion aggUnion = options.aggregations.getAt(i);
            io.deephaven.proto.backplane.grpc.Aggregation.Builder agg = aggUnion.makeAggregation();
            aggBuilder.addAggregations(agg);
        }

        if (options.initialGroups != null) {
            aggBuilder.setInitialGroupsId(TableReference.newBuilder()
                    .setTicket(options.initialGroups.typedTicket().getTicket())
                    .build());
            if (options.groupByColumns == null || options.groupByColumns.getLength() == 0) {
                throw new IllegalArgumentException("initialGroups requires groupByColumns");
            }
        }
        if (options.groupByColumns != null) {
            aggBuilder.addAllGroupByColumns(columnsToNameList(options.groupByColumns));
        }
        return call(ticket, BatchTableRequest.Operation.newBuilder().setAggregate(aggBuilder));
    }

    // TODO options
    // @JsMethod
    // JsTableOperations updateBy();

    @JsMethod
    default JsTableOperations selectDistinct(ReadonlyArray<Column.ColumnOrName> columns) {
        Ticket ticket = getConnection().getTickets().newExportTicket();

        return call(ticket, BatchTableRequest.Operation.newBuilder().setSelect(SelectOrUpdateRequest.newBuilder()
                .addAllColumnSpecs(columnsToNameList(columns))
                .setSourceId(TableReference.newBuilder()
                        .setTicket(typedTicket().getTicket())
                        .build())
                .setResultId(ticket)));
    }

    /**
     * Counts the number of rows in each group, storing the result in a new column.
     *
     * @param columnName the name of the new column to hold the row counts
     * @param groupByColumns columns to group by; if omitted, counts all rows into a single result
     * @return a new table with a count column and optional group-by columns
     */
    @JsMethod
    default JsTableOperations countBy(String columnName,
            @JsOptional @JsNullable ReadonlyArray<Column.ColumnOrName> groupByColumns) {
        Ticket ticket = getConnection().getTickets().newExportTicket();

        AggregateRequest.Builder aggBuilder = AggregateRequest.newBuilder()
                .setResultId(ticket)
                .setSourceId(TableReference.newBuilder()
                        .setTicket(typedTicket().getTicket())
                        .build())
                .addAggregations(io.deephaven.proto.backplane.grpc.Aggregation.newBuilder()
                        .setCount(io.deephaven.proto.backplane.grpc.Aggregation.AggregationCount.newBuilder()
                                .setColumnName(columnName)));
        if (groupByColumns != null) {
            aggBuilder.addAllGroupByColumns(columnsToNameList(groupByColumns));
        }
        return call(ticket, BatchTableRequest.Operation.newBuilder().setAggregate(aggBuilder));
    }

    /**
     * Returns the first row of each group.
     *
     * @param groupByColumns columns to group by; if omitted, returns the first row of the table
     * @return a new table with the first row from each group
     */
    @JsMethod
    default JsTableOperations firstBy(@JsOptional @JsNullable ReadonlyArray<Column.ColumnOrName> groupByColumns) {
        return aggAllBySpec(AggSpec.newBuilder().setFirst(AggSpec.AggSpecFirst.getDefaultInstance()), groupByColumns);
    }

    /**
     * Returns the last row of each group.
     *
     * @param groupByColumns columns to group by; if omitted, returns the last row of the table
     * @return a new table with the last row from each group
     */
    @JsMethod
    default JsTableOperations lastBy(
            @JsOptional @JsNullable ReadonlyArray<Column.ColumnOrName> groupByColumns) {
        return aggAllBySpec(AggSpec.newBuilder().setLast(AggSpec.AggSpecLast.getDefaultInstance()), groupByColumns);
    }

    /**
     * Returns the minimum value of each column within each group.
     *
     * @param groupByColumns columns to group by; if omitted, computes the minimum across all rows
     * @return a new table with the minimum values
     */
    @JsMethod
    default JsTableOperations minBy(
            @JsOptional @JsNullable ReadonlyArray<Column.ColumnOrName> groupByColumns) {
        return aggAllBySpec(AggSpec.newBuilder().setMin(AggSpec.AggSpecMin.getDefaultInstance()), groupByColumns);
    }

    /**
     * Returns the maximum value of each column within each group.
     *
     * @param groupByColumns columns to group by; if omitted, computes the maximum across all rows
     * @return a new table with the maximum values
     */
    @JsMethod
    default JsTableOperations maxBy(
            @JsOptional @JsNullable ReadonlyArray<Column.ColumnOrName> groupByColumns) {
        return aggAllBySpec(AggSpec.newBuilder().setMax(AggSpec.AggSpecMax.getDefaultInstance()), groupByColumns);
    }

    /**
     * Computes the sum of each column within each group.
     *
     * @param groupByColumns columns to group by; if omitted, computes the sum across all rows
     * @return a new table with the sums
     */
    @JsMethod
    default JsTableOperations sumBy(
            @JsOptional @JsNullable ReadonlyArray<Column.ColumnOrName> groupByColumns) {
        return aggAllBySpec(AggSpec.newBuilder().setSum(AggSpec.AggSpecSum.getDefaultInstance()), groupByColumns);
    }

    /**
     * Computes the arithmetic mean of each column within each group.
     *
     * @param groupByColumns columns to group by; if omitted, computes the average across all rows
     * @return a new table with the averages
     */
    @JsMethod
    default JsTableOperations avgBy(
            @JsOptional @JsNullable ReadonlyArray<Column.ColumnOrName> groupByColumns) {
        return aggAllBySpec(AggSpec.newBuilder().setAvg(AggSpec.AggSpecAvg.getDefaultInstance()), groupByColumns);
    }

    /**
     * Computes the median of each column within each group. When the group size is even, averages the two middle values
     * for numeric types.
     *
     * @param groupByColumns columns to group by; if omitted, computes the median across all rows
     * @return a new table with the medians
     */
    @JsMethod
    default JsTableOperations medianBy(
            @JsOptional @JsNullable ReadonlyArray<Column.ColumnOrName> groupByColumns) {
        return aggAllBySpec(AggSpec.newBuilder().setMedian(
                AggSpec.AggSpecMedian.newBuilder().setAverageEvenlyDivided(true)), groupByColumns);
    }

    /**
     * Computes the sample standard deviation of each column within each group, using Bessel's correction.
     *
     * @param groupByColumns columns to group by; if omitted, computes the standard deviation across all rows
     * @return a new table with the standard deviations
     */
    @JsMethod
    default JsTableOperations stdBy(
            @JsOptional @JsNullable ReadonlyArray<Column.ColumnOrName> groupByColumns) {
        return aggAllBySpec(AggSpec.newBuilder().setStd(AggSpec.AggSpecStd.getDefaultInstance()), groupByColumns);
    }

    /**
     * Computes the sample variance of each column within each group, using Bessel's correction.
     *
     * @param groupByColumns columns to group by; if omitted, computes the variance across all rows
     * @return a new table with the variances
     */
    @JsMethod
    default JsTableOperations varBy(
            @JsOptional @JsNullable ReadonlyArray<Column.ColumnOrName> groupByColumns) {
        return aggAllBySpec(AggSpec.newBuilder().setVar(AggSpec.AggSpecVar.getDefaultInstance()), groupByColumns);
    }

    /**
     * Computes the sum of absolute values of each column within each group.
     *
     * @param groupByColumns columns to group by; if omitted, computes the absolute sum across all rows
     * @return a new table with the absolute sums
     */
    @JsMethod
    default JsTableOperations absSumBy(
            @JsOptional @JsNullable ReadonlyArray<Column.ColumnOrName> groupByColumns) {
        return aggAllBySpec(AggSpec.newBuilder().setAbsSum(AggSpec.AggSpecAbsSum.getDefaultInstance()), groupByColumns);
    }

    /**
     * Computes the weighted sum of each column within each group. Each value is multiplied by the corresponding weight,
     * and the results are summed.
     *
     * @param weightColumn the column containing the weights
     * @param groupByColumns columns to group by; if omitted, computes the weighted sum across all rows
     * @return a new table with the weighted sums
     */
    @JsMethod
    default JsTableOperations wsumBy(Column.ColumnOrName weightColumn,
            @JsOptional @JsNullable ReadonlyArray<Column.ColumnOrName> groupByColumns) {
        return aggAllBySpec(AggSpec.newBuilder().setWeightedSum(
                AggSpec.AggSpecWeighted.newBuilder().setWeightColumn(weightColumn.columnName())), groupByColumns);
    }

    /**
     * Computes the weighted average of each column within each group. Each value is multiplied by the corresponding
     * weight, and the result is the sum of weighted values divided by the sum of weights.
     *
     * @param weightColumn the column containing the weights
     * @param groupByColumns columns to group by; if omitted, computes the weighted average across all rows
     * @return a new table with the weighted averages
     */
    @JsMethod
    default JsTableOperations wavgBy(Column.ColumnOrName weightColumn,
            @JsOptional @JsNullable ReadonlyArray<Column.ColumnOrName> groupByColumns) {
        return aggAllBySpec(AggSpec.newBuilder().setWeightedAvg(
                AggSpec.AggSpecWeighted.newBuilder().setWeightColumn(weightColumn.columnName())), groupByColumns);
    }

    @TsInterface
    @JsType(namespace = "dh")
    class UngroupOptions {
        /**
         * Indicates if the ungrouped table should allow disparate sized arrays filling shorter columns with null
         * values. If false, then all arrays must be the same length. Defaults to false.
         */
        @JsNullable
        public Boolean nullFill;

        /**
         * Specific columns to ungroup. If absent or empty, all columns in the table will be ungrouped.
         */
        @JsNullable
        public ReadonlyArray<Column.ColumnOrName> columnsToUngroup;
    }

    /**
     * Ungroups a table by expanding columns of arrays or vectors into columns of singular values, creating one row in
     * the output table for each value in the columns to be ungrouped. Columns that are not ungrouped have their values
     * duplicatd in each output row corresponding to a given input row.
     *
     * @param options options to specify the behavior of the ungroup operation
     * @return an ungrouped table
     */
    @JsMethod
    default JsPendingTable ungroup(@JsOptional @JsNullable UngroupOptions options) {
        Ticket ticket = getConnection().getTickets().newExportTicket();

        return call(ticket, BatchTableRequest.Operation.newBuilder().setUngroup(UngroupRequest.newBuilder()
                .setResultId(ticket)
                .setSourceId(TableReference.newBuilder()
                        .setTicket(typedTicket().getTicket())
                        .build())
                .setNullFill(options != null && options.nullFill != null && options.nullFill)
                .addAllColumnsToUngroup(options != null && options.columnsToUngroup != null
                        ? columnsToNameList(options.columnsToUngroup)
                        : Collections.emptyList())));
    }

    /**
     * Creates a new table without the specified columns.
     *
     * @param columnsToDrop the columns to drop. Should not be empty.
     * @return a new table without those columns
     */
    @JsMethod
    default JsPendingTable dropColumns(ReadonlyArray<Column.ColumnOrName> columnsToDrop) {
        Ticket ticket = getConnection().getTickets().newExportTicket();

        return call(ticket, BatchTableRequest.Operation.newBuilder().setDropColumns(DropColumnsRequest.newBuilder()
                .setResultId(ticket)
                .setSourceId(TableReference.newBuilder()
                        .setTicket(typedTicket().getTicket()))
                .addAllColumnNames(columnsToNameList(columnsToDrop))));
    }

    private static List<String> columnsToNameList(ReadonlyArray<Column.ColumnOrName> columns) {
        return columns.asList().stream().map(Column.ColumnOrName.COLUMN_NAME).toList();
    }

    /**
     * Helper for convenience aggregation methods that build an {@link AggregateAllRequest} with a pre-built
     * {@link AggSpec}.
     */
    private JsTableOperations aggAllBySpec(AggSpec.Builder spec,
            ReadonlyArray<Column.ColumnOrName> groupByColumns) {
        Ticket ticket = getConnection().getTickets().newExportTicket();
        return call(ticket, BatchTableRequest.Operation.newBuilder().setAggregateAll(AggregateAllRequest.newBuilder()
                .setResultId(ticket)
                .setSourceId(TableReference.newBuilder()
                        .setTicket(typedTicket().getTicket())
                        .build())
                .setSpec(spec)
                .addAllGroupByColumns(
                        groupByColumns != null ? columnsToNameList(groupByColumns) : Collections.emptyList())));
    }

    // @JsMethod
    // JsTableOperations meta();

    // wouldMatch

    // format methods

    // slice/head/tail pct

    // @JsMethod
    // JsTableOperations headBy(double nRows, JsArray<String> groupByColumnNames);

    // @JsMethod
    // JsTableOperations tailBy(double nRows, JsArray<String> groupByColumnNames);

    // applyToAllBy

    // partitionBy, partitionedAggBy

    // tree

    // rollup

    // coalesce

    // getSubTable

    /**
     * Creates a version of this table with a flat rowset.
     * 
     * @return a new table with a flat rowset
     */
    @JsMethod
    default JsTableOperations flatten() {
        Ticket ticket = getConnection().getTickets().newExportTicket();

        return call(ticket, BatchTableRequest.Operation.newBuilder().setFlatten(FlattenRequest.getDefaultInstance()));
    }

    // TODO options
    // @JsMethod
    // JsTableOperations downsample();
}
