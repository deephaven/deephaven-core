//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsLiteral;
import com.vertispan.tsdefs.annotations.TsName;
import com.vertispan.tsdefs.annotations.TsTypeRef;
import com.vertispan.tsdefs.annotations.TsUnion;
import com.vertispan.tsdefs.annotations.TsUnionMember;
import elemental2.core.ReadonlyArray;
import elemental2.promise.Promise;
import io.deephaven.proto.backplane.grpc.AggSpec;
import io.deephaven.proto.backplane.grpc.AggregateAllRequest;
import io.deephaven.proto.backplane.grpc.AggregateRequest;
import io.deephaven.proto.backplane.grpc.AjRajTablesRequest;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.CrossJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.DropColumnsRequest;
import io.deephaven.proto.backplane.grpc.ExactJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.FilterTableRequest;
import io.deephaven.proto.backplane.grpc.FlattenRequest;
import io.deephaven.proto.backplane.grpc.HeadOrTailRequest;
import io.deephaven.proto.backplane.grpc.NaturalJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.RangeJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.RunChartDownsampleRequest;
import io.deephaven.proto.backplane.grpc.SelectDistinctRequest;
import io.deephaven.proto.backplane.grpc.SelectOrUpdateRequest;
import io.deephaven.proto.backplane.grpc.SliceRequest;
import io.deephaven.proto.backplane.grpc.SnapshotTableRequest;
import io.deephaven.proto.backplane.grpc.SnapshotWhenTableRequest;
import io.deephaven.proto.backplane.grpc.SortDescriptor;
import io.deephaven.proto.backplane.grpc.SortTableRequest;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.backplane.grpc.UngroupRequest;
import io.deephaven.proto.backplane.grpc.UpdateByRequest;
import io.deephaven.proto.backplane.grpc.WhereInRequest;
import io.deephaven.web.client.api.agg.*;
import io.deephaven.web.client.api.filter.FilterCondition;
import io.deephaven.web.client.api.filter.FilterValue;
import io.deephaven.web.client.api.updateby.*;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsNullable;
import org.jspecify.annotations.Nullable;
import jsinterop.annotations.JsOptional;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static io.deephaven.web.client.api.JsTableOperations.NaturalJoinType.*;

/**
 * Describes operations that can be performed on a table without retrieving data or metadata, allowing these operations
 * to potentially be chained with no direct interaction with a promise.
 * <p>
 * Every {@code TableOperations} is immutable: it stands for one server-side table, fixed when it is created, and
 * nothing done elsewhere changes which table that is. An operation never alters the table it is called on; it returns a
 * new pending table, so the caller decides the order of operations by how they chain them. This holds for the
 * {@link JsTable#operations()} of a {@link JsTable} too: it stands for the table as it was when taken, and a later
 * filter or sort applied to that {@code Table} is not seen by it.
 * <p>
 * Each table holds server resources until it is closed, by whoever owns it:
 * <ul>
 * <li>Awaiting a {@link JsPendingTable} gives the table it produces, and {@link #resolve()} gives it as a
 * {@link JsResolvedTable}. Repeating either gives the same instance, as awaiting a promise twice does. A pending table
 * has no owner: awaiting or resolving it retains the table it produces, one instance shared by every await and
 * {@code resolve()} of it, and whoever does so is responsible for closing it.</li>
 * <li>A {@code ResolvedTable} cannot be awaited, since awaiting a thenable would also await what it resolves to.
 * {@link #resolve()} is how code reaches the table from any {@code TableOperations}, pending or resolved: on a
 * {@code ResolvedTable} it gives that same table.</li>
 * <li>Awaiting or resolving it again gives that same instance and does not change who is responsible for it; whether
 * code that receives a table may close it is agreed between that code and its caller. Code given a
 * {@code TableOperations} should only chain on it: if it is a pending table, resolving it would retain a table the
 * caller cannot reach to close. Code that reads a table should take a {@code ResolvedTable}, which the sender resolves
 * before handing it over, and code that keeps one it was lent can call {@link JsResolvedTable#copy()} for a new
 * instance with its own lifetime.</li>
 * </ul>
 * To hand one table to several owners, resolve it once, give each a copy, and close your own:
 *
 * <pre>
 * const t = await pending;
 * popupA.show(t.copy());
 * popupB.show(t.copy());
 * t.close();
 * </pre>
 * <p>
 * Each method checks its arguments when called and throws for invalid ones. A failure of the operation itself, on the
 * server or because a table it uses was canceled, failed, or used too late, is reported by rejecting the pending table
 * it returns; see {@link JsPendingTable} for how long a pending table may be used.
 * <p>
 *
 * <pre>
 * async function process(table: dh.TableOperations) {
 *   const result = await table
 *       .where([dh.FilterValue.ofColumn("A").greaterThan(dh.FilterValue.ofNumber(5))])
 *       .sort([{column: "B", direction: "DESC"}])
 *       .head(10);
 *   // Any use of methods on table after this point would be potentially invalid, we didn't retain it ourselves,
 *   // and don't know if the caller did either.
 *   try {
 *       const a = result.findColumn("A");
 *       const data = await result.createSnapshot({
 *           rows: {first: 0, last: 9},
 *           columns: [a, "B"]
 *       });
 *       return data.getData(0, a);
 *   } finally {
 *       result.close();
 *   }
 * }
 * </pre>
 * 
 * This function only chains on the table it is given, so it works whether the caller passes a pending or a resolved
 * table, and closes only what its own chain produced. To keep a table for later, take a {@code ResolvedTable} and copy
 * it: this class owns its copy and each filtered table it makes, and closes each when replaced or when it is closed.
 *
 * <pre>
 * class SwapFilters {
 *     private readonly table: dh.ResolvedTable;
 *     private filteredTable: Promise&lt;dh.ResolvedTable> | null = null;
 *     constructor(table: dh.ResolvedTable) {
 *         // The caller keeps its own table; this class owns the copy
 *         this.table = table.copy();
 *     }
 *     changeFilters(filters: ReadonlyArray&lt;dh.FilterCondition>) {
 *         if (this.filteredTable) {
 *             // A table that failed has nothing to close
 *             this.filteredTable.then(t => t.close(), () => {});
 *         }
 *         this.filteredTable = Promise.resolve(this.table.where(filters));
 *     }
 *     async loadData(): Promise&lt;dh.TableData> {
 *         if (!this.filteredTable) {
 *             throw new Error("no filters set");
 *         }
 *         const t = await this.filteredTable;
 *         return t.createSnapshot({
 *             rows: {first:0, last:1_000_000},
 *             columns: t.columns
 *         })
 *     }
 *     close() {
 *         this.table.close();
 *         if (this.filteredTable) {
 *             this.filteredTable.then(t => t.close(), () => {});
 *         }
 *     }
 * }
 * </pre>
 */
@TsName(namespace = "dh", name = "TableOperations")
@TsInterface
public interface JsTableOperations extends ServerObject {

    /**
     * The table these operations stand for, as a {@link JsResolvedTable} whose data can be read and on which more
     * operations can be chained. On a {@link JsPendingTable}, including the {@link JsTable#operations()} of a
     * {@link JsTable}, it is the table the pending table produces, whatever awaiting it gives; on a resolved table, it
     * is that table itself. Repeated calls give the same promise and the same table. Resolving does not change who owns
     * the table; use {@link JsResolvedTable#copy()} for an instance with its own lifetime. A pending table that was
     * canceled, or is resolved after the task that created it, rejects instead, as does a fetched one that was already
     * awaited.
     *
     * @param options optionally, a {@code signal} that cancels a pending table when aborted
     * @return a promise of the resolved table
     */
    @JsMethod
    Promise<JsResolvedTable> resolve(@JsOptional @JsNullable @TsTypeRef(ResolveOptions.class) Object options);

    /**
     * As {@link #resolve(Object)} with no options.
     */
    @JsIgnore
    default Promise<JsResolvedTable> resolve() {
        return resolve(null);
    }

    /**
     * Internal method to make the async call to the server.
     *
     * @param resultId the ticket that the server will populate with this result
     * @param operation the operation to perform
     * @param operand the other table the operation reads, if any; a pending one is checked and held as the source is
     * @return a table operations instance that more calls can be chained on, or can be awaited
     */
    JsPendingTable<JsResolvedTable> call(Ticket resultId, BatchTableRequest.Operation.Builder operation,
            @Nullable JsTableReference operand);

    /** As {@link #call(Ticket, BatchTableRequest.Operation.Builder, JsTableReference)}, with no other table. */
    default JsPendingTable<JsResolvedTable> call(Ticket resultId, BatchTableRequest.Operation.Builder operation) {
        return call(resultId, operation, null);
    }

    /**
     * Creates a table with the given number of rows from the beginning of this table.
     *
     * @param size the number of rows to keep from the beginning of this table
     * @return a new table with at most that many rows
     */
    @JsMethod
    default JsPendingTable<JsResolvedTable> head(TableData.RowPositionUnion size) {
        Ticket ticket = getConnection().getTickets().newExportTicket();

        return call(ticket, BatchTableRequest.Operation.newBuilder()
                .setHead(HeadOrTailRequest.newBuilder()
                        .setSourceId(TableReference.newBuilder()
                                .setTicket(typedTicket().getTicket())
                                .build())
                        .setResultId(ticket)
                        .setNumRows(size.asInt())));
    }

    /**
     * Creates a table with the given number of rows from the end of this table.
     *
     * @param size the number of rows to keep from the end of this table
     * @return a new table with at most that many rows
     */
    @JsMethod
    default JsPendingTable<JsResolvedTable> tail(TableData.RowPositionUnion size) {
        Ticket ticket = getConnection().getTickets().newExportTicket();

        return call(ticket, BatchTableRequest.Operation.newBuilder()
                .setTail(HeadOrTailRequest.newBuilder()
                        .setSourceId(TableReference.newBuilder()
                                .setTicket(typedTicket().getTicket())
                                .build())
                        .setResultId(ticket)
                        .setNumRows(size.asInt())));
    }

    /**
     * Creates a table with all the rows of this table in reverse order.
     *
     * @return a new table with the rows in reverse order
     */
    @JsMethod
    default JsPendingTable<JsResolvedTable> reverse() {
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

    /**
     * A server-side snapshot of the entire table as of the time the request is processed. The result does not update.
     * This is what {@link JsTable#freeze()} does; the name here follows the other Deephaven clients.
     *
     * @return a new, non-ticking table with the same contents as this table at that moment
     */
    @JsMethod
    default JsPendingTable<JsResolvedTable> snapshot() {
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
    default JsPendingTable<JsResolvedTable> snapshotWhen(JsTableReference trigger,
            SnapshotWhenOptions options) {
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
                        options.stampColumns != null ? options.stampColumns.asList() : Collections.emptyList())),
                trigger);
    }

    /**
     * Sorts the table by the given columns.
     *
     * @param sorts the sorts to apply, in order of precedence
     * @return a sorted table
     */
    // omitting sortDescending, sort objects have this for us
    @JsMethod
    default JsPendingTable<JsResolvedTable> sort(ReadonlyArray<Sort.SortUnion> sorts) {
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
     * @param conditions the conditions to filter on; {@link FilterValue#ofColumn} names a column without a resolved
     *        table
     * @return a filtered table
     */
    @JsMethod
    default JsPendingTable<JsResolvedTable> where(ReadonlyArray<FilterCondition> conditions) {
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
    default JsPendingTable<JsResolvedTable> whereIn(JsTableReference rightTable,
            ReadonlyArray<Column.ColumnOrName> columnsToMatch) {
        Ticket ticket = getConnection().getTickets().newExportTicket();

        return call(ticket, BatchTableRequest.Operation.newBuilder().setWhereIn(WhereInRequest.newBuilder()
                .setResultId(ticket)
                .setLeftId(TableReference.newBuilder()
                        .setTicket(typedTicket().getTicket()))
                .setRightId(TableReference.newBuilder()
                        .setTicket(rightTable.typedTicket().getTicket()))
                .addAllColumnsToMatch(columnsToNameList(columnsToMatch))
                .setInverted(false)
                .build()), rightTable);
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
    default JsPendingTable<JsResolvedTable> whereNotIn(JsTableReference rightTable,
            ReadonlyArray<Column.ColumnOrName> columnsToMatch) {
        Ticket ticket = getConnection().getTickets().newExportTicket();

        return call(ticket, BatchTableRequest.Operation.newBuilder().setWhereIn(WhereInRequest.newBuilder()
                .setResultId(ticket)
                .setLeftId(TableReference.newBuilder()
                        .setTicket(typedTicket().getTicket()))
                .setRightId(TableReference.newBuilder()
                        .setTicket(rightTable.typedTicket().getTicket()))
                .addAllColumnsToMatch(columnsToNameList(columnsToMatch))
                .setInverted(true)
                .build()), rightTable);
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
     * @param lastPositionExclusive the last position to include in the result
     * @return a new table with the subset of rows from the original table
     */
    @JsMethod
    default JsPendingTable<JsResolvedTable> slice(TableData.RowPositionUnion firstPositionInclusive,
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
    default JsPendingTable<JsResolvedTable> view(ReadonlyArray<JsTable.CustomColumnArgUnionType> columns) {
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
    default JsPendingTable<JsResolvedTable> updateView(
            ReadonlyArray<JsTable.CustomColumnArgUnionType> columns) {
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
    default JsPendingTable<JsResolvedTable> update(ReadonlyArray<JsTable.CustomColumnArgUnionType> columns) {
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
    default JsPendingTable<JsResolvedTable> lazyUpdate(
            ReadonlyArray<JsTable.CustomColumnArgUnionType> columns) {
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
    default JsPendingTable<JsResolvedTable> select(ReadonlyArray<JsTable.CustomColumnArgUnionType> columns) {
        Ticket ticket = getConnection().getTickets().newExportTicket();

        return call(ticket, BatchTableRequest.Operation.newBuilder().setSelect(selectOrUpdate(columns)
                .setSourceId(TableReference.newBuilder()
                        .setTicket(typedTicket().getTicket())
                        .build())
                .setResultId(ticket)));
    }

    /**
     * Specifies the type of natural join to perform, specifically how to handle duplicate and missing right hand table
     * rows.
     *
     * <ul>
     * <li>ERROR_ON_DUPLICATE - Throw an error if a duplicate right hand table row is found. This is the default
     * behavior if not specified.</li>
     * <li>FIRST_MATCH - Match the first right hand table row and ignore later duplicates.</li>
     * <li>LAST_MATCH - Match the last right hand table row and ignore earlier duplicates.</li>
     * <li>EXACTLY_ONE_MATCH - Match exactly one right hand table row; throw an error if there are zero or more than one
     * matches.</li>
     * </ul>
     */
    @TsUnion(anonymous = false, name = "NaturalJoinType", namespace = "dh")
    @JsType(namespace = JsPackage.GLOBAL, name = "String", isNative = true)
    interface NaturalJoinType {
        @TsLiteral
        @TsUnionMember
        @JsOverlay
        String ERROR_ON_DUPLICATE = "ERROR_ON_DUPLICATE";
        @TsLiteral
        @TsUnionMember
        @JsOverlay
        String FIRST_MATCH = "FIRST_MATCH";
        @TsLiteral
        @TsUnionMember
        @JsOverlay
        String LAST_MATCH = "LAST_MATCH";
        @TsLiteral
        @TsUnionMember
        @JsOverlay
        String EXACTLY_ONE_MATCH = "EXACTLY_ONE_MATCH";
    }

    /**
     * Perform a natural-join with the {@code rightTable}.
     *
     * @param rightTable The right side table on the join.
     * @param columnsToMatch The match pair conditions.
     * @param columnsToAdd The columns from the right side that need to be added to the left side as a result of the
     *        match; if omitted, all non-match columns are added.
     * @param joinType The type of join to perform
     * @return the natural-joined table
     */
    // TODO add an options type for these various flags?
    @JsMethod
    default JsPendingTable<JsResolvedTable> naturalJoin(JsTableReference rightTable,
            ReadonlyArray<JsTable.MatchPairUnion> columnsToMatch,
            @JsOptional @JsNullable ReadonlyArray<JsTable.MatchPairUnion> columnsToAdd,
            @JsOptional @JsNullable NaturalJoinType joinType) {
        Ticket ticket = getConnection().getTickets().newExportTicket();
        NaturalJoinTablesRequest.Builder join = NaturalJoinTablesRequest.newBuilder()
                .setResultId(ticket)
                .setLeftId(TableReference.newBuilder()
                        .setTicket(typedTicket().getTicket())
                        .build())
                .setRightId(TableReference.newBuilder()
                        .setTicket(rightTable.typedTicket().getTicket())
                        .build())
                .addAllColumnsToMatch(JsTable.MatchPairUnion.toStringArray(columnsToMatch))
                .addAllColumnsToAdd(columnsToAdd != null ? JsTable.MatchPairUnion.toStringArray(columnsToAdd)
                        : Collections.emptyList());
        if (joinType != null) {
            switch (joinType.toString()) {
                case ERROR_ON_DUPLICATE:
                    join.setJoinType(NaturalJoinTablesRequest.JoinType.ERROR_ON_DUPLICATE);
                    break;
                case FIRST_MATCH:
                    join.setJoinType(NaturalJoinTablesRequest.JoinType.FIRST_MATCH);
                    break;
                case LAST_MATCH:
                    join.setJoinType(NaturalJoinTablesRequest.JoinType.LAST_MATCH);
                    break;
                case EXACTLY_ONE_MATCH:
                    join.setJoinType(NaturalJoinTablesRequest.JoinType.EXACTLY_ONE_MATCH);
                    break;
            }
        }
        return call(ticket, BatchTableRequest.Operation.newBuilder().setNaturalJoin(join), rightTable);
    }

    /**
     * Performs an exact join of this table with the right table. Each row in this table must have exactly one matching
     * row in the right table based on the match columns; if no match is found, an error is raised.
     *
     * @param rightTable the table to join with
     * @param columnsToMatch the columns to match on, in "LeftCol=RightCol" format
     * @param columnsToAdd the columns to add from the right table; if omitted, all non-match columns are added
     * @return a new table with the joined columns
     */
    @JsMethod
    default JsPendingTable<JsResolvedTable> exactJoin(JsTableReference rightTable,
            ReadonlyArray<JsTable.MatchPairUnion> columnsToMatch,
            @JsOptional @JsNullable ReadonlyArray<JsTable.MatchPairUnion> columnsToAdd) {
        Ticket ticket = getConnection().getTickets().newExportTicket();
        ExactJoinTablesRequest.Builder request = ExactJoinTablesRequest.newBuilder()
                .setResultId(ticket)
                .setLeftId(TableReference.newBuilder()
                        .setTicket(typedTicket().getTicket())
                        .build())
                .setRightId(TableReference.newBuilder()
                        .setTicket(rightTable.typedTicket().getTicket())
                        .build())
                .addAllColumnsToMatch(JsTable.MatchPairUnion.toStringArray(columnsToMatch))
                .addAllColumnsToAdd(columnsToAdd != null ? JsTable.MatchPairUnion.toStringArray(columnsToAdd)
                        : Collections.emptyList());
        return call(ticket, BatchTableRequest.Operation.newBuilder().setExactJoin(request), rightTable);
    }

    /**
     * Performs a cross join (Cartesian product) of this table with the right table, filtered to matching rows. Each row
     * in this table is paired with every matching row in the right table.
     *
     * @param rightTable the table to join with
     * @param columnsToMatch the columns to match on, in "LeftCol=RightCol" format
     * @param columnsToAdd the columns to add from the right table; if omitted, all non-match columns are added
     * @param reserveBits the number of bits of key-space to initially reserve per group; default is 10
     * @return a new table with the joined columns
     */
    @JsMethod
    default JsPendingTable<JsResolvedTable> join(JsTableReference rightTable,
            ReadonlyArray<JsTable.MatchPairUnion> columnsToMatch,
            @JsOptional @JsNullable ReadonlyArray<JsTable.MatchPairUnion> columnsToAdd,
            @JsOptional @JsNullable Double reserveBits) {
        Ticket ticket = getConnection().getTickets().newExportTicket();
        CrossJoinTablesRequest.Builder request = CrossJoinTablesRequest.newBuilder()
                .setResultId(ticket)
                .setLeftId(TableReference.newBuilder()
                        .setTicket(typedTicket().getTicket())
                        .build())
                .setRightId(TableReference.newBuilder()
                        .setTicket(rightTable.typedTicket().getTicket())
                        .build())
                .addAllColumnsToMatch(JsTable.MatchPairUnion.toStringArray(columnsToMatch))
                .addAllColumnsToAdd(columnsToAdd != null ? JsTable.MatchPairUnion.toStringArray(columnsToAdd)
                        : Collections.emptyList());
        if (reserveBits != null) {
            request.setReserveBits((int) (double) reserveBits);
        }
        return call(ticket, BatchTableRequest.Operation.newBuilder().setCrossJoin(request), rightTable);
    }

    /**
     * Performs an as-of join of this table with the right table: each row takes the right table's row with the closest
     * as-of value at or before its own.
     *
     * @param rightTable the table to join with
     * @param columnsToMatch the columns to match on. All but the last are exact matches ({@code "LeftCol=RightCol"} or
     *        a name in both tables). The last is the as-of match: {@code "LeftCol>=RightCol"},
     *        {@code "LeftCol>RightCol"}, or a name in both tables, which means {@code >=}.
     * @param columnsToAdd the columns to add from the right table; if omitted, all non-match columns are added
     * @return a new table with the joined columns
     */
    @JsMethod
    default JsPendingTable<JsResolvedTable> aj(JsTableReference rightTable,
            ReadonlyArray<JsTable.MatchPairUnion> columnsToMatch,
            @JsNullable @JsOptional ReadonlyArray<JsTable.MatchPairUnion> columnsToAdd) {
        Ticket ticket = getConnection().getTickets().newExportTicket();

        AjRajTablesRequest.Builder builder = makeAjReq(rightTable, columnsToMatch, columnsToAdd, ticket);
        return call(ticket, BatchTableRequest.Operation.newBuilder().setAj(builder), rightTable);
    }

    /**
     * Performs a reverse as-of join of this table with the right table: each row takes the right table's row with the
     * closest as-of value at or after its own.
     *
     * @param rightTable the table to join with
     * @param columnsToMatch the columns to match on. All but the last are exact matches ({@code "LeftCol=RightCol"} or
     *        a name in both tables). The last is the as-of match: {@code "LeftCol<=RightCol"},
     *        <code>"LeftCol&lt;RightCol"</code>, or a name in both tables, which means {@code <=}.
     * @param columnsToAdd the columns to add from the right table; if omitted, all non-match columns are added
     * @return a new table with the joined columns
     */
    @JsMethod
    default JsPendingTable<JsResolvedTable> raj(JsTableReference rightTable,
            ReadonlyArray<JsTable.MatchPairUnion> columnsToMatch,
            @JsNullable @JsOptional ReadonlyArray<JsTable.MatchPairUnion> columnsToAdd) {
        Ticket ticket = getConnection().getTickets().newExportTicket();

        AjRajTablesRequest.Builder builder = makeAjReq(rightTable, columnsToMatch, columnsToAdd, ticket);
        return call(ticket, BatchTableRequest.Operation.newBuilder().setRaj(builder), rightTable);
    }

    private AjRajTablesRequest.Builder makeAjReq(JsTableReference table,
            ReadonlyArray<JsTable.MatchPairUnion> matches,
            ReadonlyArray<JsTable.MatchPairUnion> columnsToAdd, Ticket ticket) {
        AjRajTablesRequest.Builder builder = AjRajTablesRequest.newBuilder()
                .setResultId(ticket)
                .setLeftId(TableReference.newBuilder()
                        .setTicket(typedTicket().getTicket())
                        .build())
                .setRightId(TableReference.newBuilder()
                        .setTicket(table.typedTicket().getTicket())
                        .build());
        List<String> matchStrings = JsTable.MatchPairUnion.toStringArray(matches);
        if (matchStrings.isEmpty()) {
            throw new IllegalArgumentException("aj and raj require at least one match column");
        }
        builder.addAllExactMatchColumns(matchStrings.subList(0, matchStrings.size() - 1));
        builder.setAsOfColumn(matchStrings.get(matchStrings.size() - 1));
        if (columnsToAdd != null) {
            builder.addAllColumnsToAdd(JsTable.MatchPairUnion.toStringArray(columnsToAdd));
        }
        return builder;
    }

    // @TsName(name = "RangeStartRule", namespace = "dh")
    @TsUnion
    @JsType(name = "?", namespace = JsPackage.GLOBAL, isNative = true)
    interface RangeStartRule {
        @TsUnionMember
        @TsLiteral
        @JsOverlay
        String LESS_THAN = "LESS_THAN";

        @TsUnionMember
        @TsLiteral
        @JsOverlay
        String LESS_THAN_OR_EQUAL = "LESS_THAN_OR_EQUAL";

        @TsUnionMember
        @TsLiteral
        @JsOverlay
        String LESS_THAN_OR_EQUAL_ALLOW_PRECEDING = "LESS_THAN_OR_EQUAL_ALLOW_PRECEDING";
    }

    @TsUnion
    @JsType(name = "?", namespace = JsPackage.GLOBAL, isNative = true)
    interface RangeEndRule {
        @TsUnionMember
        @TsLiteral
        @JsOverlay
        String GREATER_THAN = "GREATER_THAN";
        @TsUnionMember
        @TsLiteral
        @JsOverlay
        String GREATER_THAN_OR_EQUAL = "GREATER_THAN_OR_EQUAL";
        @TsUnionMember
        @TsLiteral
        @JsOverlay
        String GREATER_THAN_OR_EQUAL_ALLOW_FOLLOWING = "GREATER_THAN_OR_EQUAL_ALLOW_FOLLOWING";
    }



    /**
     * The range part of a {@link #rangeJoin}: a right row matches a left row when its {@code rightRangeColumn} value
     * lies between the left row's {@code leftStartColumn} and {@code leftEndColumn} values, as the two rules say.
     */
    @JsType(namespace = "dh")
    @TsInterface
    class RangeJoinMatch {
        /** The left column that starts each row's range. */
        public Column.ColumnOrName leftStartColumn;
        /** How the right values compare to the start of the range. */
        public RangeStartRule rangeStartRule;
        /** The right column whose values are matched against each range. */
        public Column.ColumnOrName rightRangeColumn;
        /** How the right values compare to the end of the range. */
        public RangeEndRule rangeEndRule;
        /** The left column that ends each row's range. */
        public Column.ColumnOrName leftEndColumn;
    }

    /**
     * Joins each row of this table to the rows of {@code rightTable} that match it exactly on {@code exactMatches} and
     * fall in its range, and aggregates those right rows into columns of the result. The result has one row per row of
     * this table.
     *
     * @param rightTable the table to join to
     * @param exactMatches columns that must be equal in both tables, as {@code "Col"} or {@code "LeftCol=RightCol"}
     * @param rangeMatch the columns and rules of the range match
     * @param aggregations the aggregations to apply to each row's matching right rows
     * @return a table with one row per row of this table, and a column for each aggregation
     */
    @JsMethod
    default JsPendingTable<JsResolvedTable> rangeJoin(JsTableReference rightTable,
            ReadonlyArray<String> exactMatches,
            RangeJoinMatch rangeMatch, ReadonlyArray<AggregationUnion> aggregations) {
        Ticket ticket = getConnection().getTickets().newExportTicket();

        RangeJoinTablesRequest.Builder request = RangeJoinTablesRequest.newBuilder()
                .setResultId(ticket)
                .setLeftId(TableReference.newBuilder()
                        .setTicket(typedTicket().getTicket())
                        .build())
                .setRightId(TableReference.newBuilder()
                        .setTicket(rightTable.typedTicket().getTicket())
                        .build());

        request.addAllExactMatchColumns(exactMatches.asList());
        request.setLeftStartColumn(rangeMatch.leftStartColumn.columnName());

        request.setRangeStartRule(switch (rangeMatch.rangeStartRule.toString()) {
            case RangeStartRule.LESS_THAN -> RangeJoinTablesRequest.RangeStartRule.LESS_THAN;
            case RangeStartRule.LESS_THAN_OR_EQUAL -> RangeJoinTablesRequest.RangeStartRule.LESS_THAN_OR_EQUAL;
            case RangeStartRule.LESS_THAN_OR_EQUAL_ALLOW_PRECEDING ->
                RangeJoinTablesRequest.RangeStartRule.LESS_THAN_OR_EQUAL_ALLOW_PRECEDING;
            default -> throw new IllegalArgumentException("Unknown range start rule: " + rangeMatch.rangeStartRule);
        });
        request.setRightRangeColumn(rangeMatch.rightRangeColumn.columnName());
        request.setRangeEndRule(switch (rangeMatch.rangeEndRule.toString()) {
            case RangeEndRule.GREATER_THAN -> RangeJoinTablesRequest.RangeEndRule.GREATER_THAN;
            case RangeEndRule.GREATER_THAN_OR_EQUAL -> RangeJoinTablesRequest.RangeEndRule.GREATER_THAN_OR_EQUAL;
            case RangeEndRule.GREATER_THAN_OR_EQUAL_ALLOW_FOLLOWING ->
                RangeJoinTablesRequest.RangeEndRule.GREATER_THAN_OR_EQUAL_ALLOW_FOLLOWING;
            default -> throw new IllegalArgumentException("Unknown range end rule: " + rangeMatch.rangeEndRule);
        });

        request.setLeftEndColumn(rangeMatch.leftEndColumn.columnName());

        for (int i = 0; i < aggregations.getLength(); i++) {
            AggregationUnion aggUnion = aggregations.getAt(i);
            io.deephaven.proto.backplane.grpc.Aggregation.Builder agg = aggUnion.makeAggregation();
            request.addAggregations(agg);
        }

        return call(ticket, BatchTableRequest.Operation.newBuilder().setRangeJoin(request), rightTable);
    }

    /**
     * Groups the table by the specified columns, accumulating the other columns into arrays. If no columns are
     * provided, the resulting table will have a single row.
     * 
     * @param groupByColumns columns to group
     * @return a table with one row per group
     */
    @JsMethod
    default JsPendingTable<JsResolvedTable> groupBy(
            @JsOptional @JsNullable ReadonlyArray<Column.ColumnOrName> groupByColumns) {
        Ticket ticket = getConnection().getTickets().newExportTicket();

        return call(ticket, BatchTableRequest.Operation.newBuilder().setAggregateAll(AggregateAllRequest.newBuilder()
                .setSourceId(TableReference.newBuilder()
                        .setTicket(typedTicket().getTicket())
                        .build())
                .setResultId(ticket)
                .setSpec(AggSpec.newBuilder().setGroup(AggSpec.AggSpecGroup.getDefaultInstance()))
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
    default JsPendingTable<JsResolvedTable> aggAllBy(AggAllByUnion aggUnion,
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
     * @param options the aggregations, and optionally the columns to group by and the groups to start with
     * @return a table with one row per group, and a column for each aggregation
     */
    @JsMethod
    default JsPendingTable<JsResolvedTable> aggBy(AggByOptions options) {
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
        return call(ticket, BatchTableRequest.Operation.newBuilder().setAggregate(aggBuilder), options.initialGroups);
    }

    /**
     * Adds columns computed over each row's preceding (and, for windowed operations, following) rows, such as
     * cumulative sums, moving averages, and exponential moving statistics, optionally per group.
     *
     * @param options the operations to apply, and optionally the columns to group by and control settings
     * @return a table with this table's rows and columns, plus a column for each operation's output
     */
    @JsMethod
    default JsPendingTable<JsResolvedTable> updateBy(UpdateByOptions options) {
        Ticket ticket = getConnection().getTickets().newExportTicket();

        UpdateByRequest.Builder builder = UpdateByRequest.newBuilder()
                .setResultId(ticket)
                .setSourceId(TableReference.newBuilder()
                        .setTicket(typedTicket().getTicket())
                        .build());

        if (options.control != null) {
            UpdateByRequest.UpdateByOptions.Builder controlBuilder = UpdateByRequest.UpdateByOptions.newBuilder();
            if (options.control.useRedirection != null) {
                controlBuilder.setUseRedirection(options.control.useRedirection);
            }
            if (options.control.chunkCapacity != null) {
                controlBuilder.setChunkCapacity((int) (double) options.control.chunkCapacity);
            }
            if (options.control.maxStaticSparseMemoryOverhead != null) {
                controlBuilder.setMaxStaticSparseMemoryOverhead(options.control.maxStaticSparseMemoryOverhead);
            }
            if (options.control.initialHashTableSize != null) {
                controlBuilder.setInitialHashTableSize((int) (double) options.control.initialHashTableSize);
            }
            if (options.control.maximumLoadFactor != null) {
                controlBuilder.setMaximumLoadFactor(options.control.maximumLoadFactor);
            }
            if (options.control.targetLoadFactor != null) {
                controlBuilder.setTargetLoadFactor(options.control.targetLoadFactor);
            }
            if (options.control.mathContext != null) {
                controlBuilder.setMathContext(options.control.mathContext.toProto());
            }
            builder.setOptions(controlBuilder);
        }

        for (int i = 0; i < options.operations.getLength(); i++) {
            UpdateByOperation op = options.operations.getAt(i);
            UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.Builder spec = op.spec.makeOperation();
            builder.addOperations(UpdateByRequest.UpdateByOperation.newBuilder()
                    .setColumn(UpdateByRequest.UpdateByOperation.UpdateByColumn.newBuilder()
                            .setSpec(spec)
                            .addAllMatchPairs(JsTable.MatchPairUnion.toStringArray(op.matchPairs))));
        }
        if (options.groupByColumns != null) {
            builder.addAllGroupByColumns(columnsToNameList(options.groupByColumns));
        }

        return call(ticket, BatchTableRequest.Operation.newBuilder().setUpdateBy(builder));
    }

    @TsInterface
    @JsType(namespace = "dh")
    class DownsampleTableOptions {
        /**
         * The column to plot along the X axis. Must be an {@code Instant} or {@code long} column.
         */
        public Column.ColumnOrName xColumn;

        /**
         * The columns to plot along the Y axis.
         */
        public ReadonlyArray<Column.ColumnOrName> yColumns;

        /**
         * The width of the plot area in pixels.
         */
        public double width;

        /**
         * The visible X range as {@code [start, end]}, both required. If absent or null, all of the data is used.
         */
        @JsNullable
        public ReadonlyArray<LongWrapper> xRange;
    }

    /**
     * Creates a table with fewer rows than this one that draws the same as a line plot at the given width, for plotting
     * large time series. Each pixel of the width keeps the rows that set its first, last, minimum and maximum values; a
     * table with fewer rows than that is returned unchanged.
     *
     * @param options the columns to plot, the plot width, and optionally the visible X range
     * @return the downsampled table
     */
    @JsMethod
    default JsPendingTable<JsResolvedTable> downsample(DownsampleTableOptions options) {
        if (options.width <= 0) {
            throw new IllegalArgumentException("width must be positive, was " + options.width);
        }
        if (options.xRange != null && (options.xRange.getLength() != 2 || options.xRange.getAt(0) == null
                || options.xRange.getAt(1) == null)) {
            throw new IllegalArgumentException("xRange must be [start, end] with both set, was " + options.xRange);
        }
        Ticket ticket = getConnection().getTickets().newExportTicket();

        RunChartDownsampleRequest.Builder request = RunChartDownsampleRequest.newBuilder()
                .setResultId(ticket)
                .setSourceId(TableReference.newBuilder()
                        .setTicket(typedTicket().getTicket())
                        .build())
                .setPixelCount((int) options.width)
                .setXColumnName(options.xColumn.columnName())
                .addAllYColumnNames(columnsToNameList(options.yColumns));
        if (options.xRange != null) {
            request.setZoomRange(RunChartDownsampleRequest.ZoomRange.newBuilder()
                    .setMinDateNanos(options.xRange.getAt(0).getWrapped())
                    .setMaxDateNanos(options.xRange.getAt(1).getWrapped()));
        }
        return call(ticket, BatchTableRequest.Operation.newBuilder().setRunChartDownsample(request));
    }

    /**
     * Creates a new table with only the distinct values of the specified columns. If no columns are specified, all
     * columns will be used.
     * 
     * @param columnNames the column names to distinct on - if empty/null, all columns are used
     * @return a new table with only the distinct values of the specified columns
     */
    @JsMethod
    default JsPendingTable<JsResolvedTable> selectDistinct(
            @JsNullable @JsOptional ReadonlyArray<Column.ColumnOrName> columnNames) {
        Ticket ticket = getConnection().getTickets().newExportTicket();
        return call(ticket, BatchTableRequest.Operation.newBuilder().setSelectDistinct(SelectDistinctRequest
                .newBuilder()
                .addAllColumnNames(columnNames == null ? Collections.emptyList() : columnsToNameList(columnNames))
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
    default JsPendingTable<JsResolvedTable> countBy(String columnName,
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
    default JsPendingTable<JsResolvedTable> firstBy(
            @JsOptional @JsNullable ReadonlyArray<Column.ColumnOrName> groupByColumns) {
        return aggAllBySpec(AggSpec.newBuilder().setFirst(AggSpec.AggSpecFirst.getDefaultInstance()), groupByColumns);
    }

    /**
     * Returns the last row of each group.
     *
     * @param groupByColumns columns to group by; if omitted, returns the last row of the table
     * @return a new table with the last row from each group
     */
    @JsMethod
    default JsPendingTable<JsResolvedTable> lastBy(
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
    default JsPendingTable<JsResolvedTable> minBy(
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
    default JsPendingTable<JsResolvedTable> maxBy(
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
    default JsPendingTable<JsResolvedTable> sumBy(
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
    default JsPendingTable<JsResolvedTable> avgBy(
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
    default JsPendingTable<JsResolvedTable> medianBy(
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
    default JsPendingTable<JsResolvedTable> stdBy(
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
    default JsPendingTable<JsResolvedTable> varBy(
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
    default JsPendingTable<JsResolvedTable> absSumBy(
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
    default JsPendingTable<JsResolvedTable> wsumBy(Column.ColumnOrName weightColumn,
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
    default JsPendingTable<JsResolvedTable> wavgBy(Column.ColumnOrName weightColumn,
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
     * duplicated in each output row corresponding to a given input row.
     *
     * @param options options to specify the behavior of the ungroup operation
     * @return an ungrouped table
     */
    @JsMethod
    default JsPendingTable<JsResolvedTable> ungroup(@JsOptional @JsNullable UngroupOptions options) {
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
    default JsPendingTable<JsResolvedTable> dropColumns(ReadonlyArray<Column.ColumnOrName> columnsToDrop) {
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
    private JsPendingTable<JsResolvedTable> aggAllBySpec(AggSpec.Builder spec,
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
    default JsPendingTable<JsResolvedTable> flatten() {
        Ticket ticket = getConnection().getTickets().newExportTicket();

        return call(ticket, BatchTableRequest.Operation.newBuilder().setFlatten(FlattenRequest.newBuilder()
                .setSourceId(TableReference.newBuilder()
                        .setTicket(typedTicket().getTicket())
                        .build())
                .setResultId(ticket)));
    }

    // TODO options
    // @JsMethod
    // JsPendingTable downsample();
}
