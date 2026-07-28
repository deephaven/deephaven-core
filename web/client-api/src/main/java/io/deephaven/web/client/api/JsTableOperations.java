//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import com.vertispan.tsdefs.annotations.TsTypeRef;
import com.vertispan.tsdefs.annotations.TsUnion;
import com.vertispan.tsdefs.annotations.TsUnionMember;
import elemental2.core.ReadonlyArray;
import elemental2.promise.Promise;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.DropColumnsRequest;
import io.deephaven.proto.backplane.grpc.FilterTableRequest;
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
import io.deephaven.web.client.api.filter.FilterCondition;
import io.deephaven.web.client.api.subscription.DataOptions;
import io.deephaven.web.client.api.subscription.TableSubscription;
import io.deephaven.web.client.api.subscription.TableViewportSubscription;
import io.deephaven.web.client.fu.PromiseLike;
import javaemul.internal.annotations.DoNotAutobox;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsOptional;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Describes operations that can be performed on a table without retrieving data or metadata, allowing these operations
 * to potentially be chained with no direct interaction with a promise.
 * <p>
 * This interface is "Thenable" or "PromiseLike", it can be awaited or have methods chained to it like a promise to
 * resolve into an object with metadata that can have data fetched from it, and will result in a
 * {@link JsResolvedTable}. If it is awaited, the resulting table must be closed to indicate that it will no longer be
 * used and server resources can be freed. A future version of this API could provide "liveness scopes" to claim/release
 * batches of tables at a time automatically.
 * <p>
 * Any instance not {@code await}ed will only last long enough for methods to be synchronously called on it, then freed
 * at the end of the event loop. Any instance that is {@code await}ed will be retained until {@code close()} is called
 * on it to free it. To optionally chain more calls to an instance, it must be retained first.
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
 *       const a = data.findColumn('A');
 *       const data = await result.createSnapshot({rows:{first:0, last:10}, columns:[a, 'B']});
 *       return data.get(0, a);
 *   } finally {
 *       result.close();
 *   }
 * }
 * </pre>
 * 
 * The caller here might be passing in an existing ResolvedTable so later operations are safe, but it isn't declared
 * that way, so we can't be sure, and the method shouldn't rely on it. Here's an example instead of retaining a table
 * for later reuse - both the initial table is retained upon creation, then each
 * 
 * <pre>
 * class SwapFilters() {
 *     constructor(table: dh.TableOperations) {
 *         this.filters: ReadonlyArray&lt;dh.FilterCondition> = [];
 *         this.table: Promise&lt;dh.ResolvedTable> = Promise.resolve(table);
 *         this.filteredTable: null|Promise&lt;dh.ResolvedTable> = null;
 *     }
 *     changeFilters(filters: ReadonlyArray&lt;dh.FilterCondition>) {
 *         if (filteredTable) {
 *             filteredTable.then(t => t.close());
 *         }
 *         this.filters = filters;
 *         // Note that this can be a surprise - returning a Thenable or PromiseLike as the result
 *         // of a resolved callback will invoke {@code then()} on it, which will retain the table.
 *         // In this case, this is what we want, but it could still be a surprise in some cases.
 *         this.filteredTable = this.table.then(t => t.where(filters));
 *     }
 *     async loadData():dh.TableData {
 *         (await this.filteredTable).createSnapshot()
 *     }
 *     close() {
 *         this.table.then(t => t.close());
 *     }
 * }
 * async function process(table: dh.TableOperations) {
 *   var table.where(this.filters);
 * }
 * </pre>
 *
 */
@TsName(namespace = "dh", name = "TableOperations")
@TsInterface
public interface JsTableOperations extends ServerObject {


    @TsName(namespace = "dh", name = "PendingTable")
    @TsInterface
    public interface JsPendingTable extends JsTableOperations, PromiseLike<JsResolvedTable> {
        @JsMethod
        void foo();
    }
    /**
     * A server-side table instance that the JS client has retained a reference to, and must be closed when no longer
     * needed. Provides access to the table's data and metadata. Supports all the methods of {@link JsTableOperations},
     * but any returned table must again be awaited, closed when finished being used.
     */
    @TsName(namespace = "dh", name = "ResolvedTable")
    @TsInterface
    public interface JsResolvedTable extends JsTableOperations {
        /**
         * The total count of rows in the table. If there is a viewport subscription active, this size will be updated
         * when the subscription updates. If not, and {@link #isUncoalesced()} is true, the size will be
         * {@link #SIZE_UNCOALESCED}. Otherwise, the size will be updated when the server's update graph processes
         * changes.
         * <p>
         * When the size changes, the {@link #EVENT_SIZECHANGED} event will be fired.
         *
         * @return the size of the table, or {@link #SIZE_UNCOALESCED} if there is no subscription and the table is
         *         uncoalesced.
         */
        @JsProperty
        double getSize();

        /**
         * The columns that are present on this table.
         *
         * @return the columns present in this table
         */
        @JsProperty
        ReadonlyArray<Column> getColumns();

        @JsMethod
        Column findColumn(String columnName);

        /**
         * The names of all attributes defined on this table.
         * 
         * @return an array of attributes defined on this table
         */
        @JsMethod
        ReadonlyArray<String> getAttributes();

        /**
         * {@code null} if no property exists, a string if it is an easily serializable property, or a {@code Promise
         * &lt;ResolvedTable&gt;} that will either resolve with a table or error out if the object can't be passed to
         * JS.
         *
         * @param attributeName the name of the attribute to read
         * @return the value of the attribute or null if none with that name exists
         */
        @JsMethod
        Object getAttribute(String attributeName);

        @JsMethod
        Promise<TableData> createSnapshot(@TsTypeRef(DataOptions.SnapshotOptions.class) Object options);

        @JsMethod
        TableViewportSubscription createViewportSubscription(
                @TsTypeRef(DataOptions.ViewportSubscriptionOptions.class) Object options);

        @JsMethod
        TableSubscription createSubscription(@TsTypeRef(DataOptions.SubscriptionOptions.class) Object options);

        /**
         * Signals that the table will no longer be used and server resources should be cleaned up. Logs a warning if
         * called more than once.
         */
        // TODO support Symbol.dispose, if present
        @JsMethod
        void close();

        /**
         * Returns another instance of this table, allowing either instance to be closed without affecting the other.
         *
         * @return a copy of this table, sharing resources on the server
         */
        @JsMethod
        Promise<JsResolvedTable> copy();
    }

    /**
     * Internal method to make the async call to the server.
     *
     * @param resultId the ticket that the server will populate with this result
     * @param operation the operation to perform
     * @return a table operations instance that more calls can be chained on, or can be awaited
     */
    JsPendingTable call(Ticket resultId, BatchTableRequest.Operation.Builder operation);

    // @Override
    // <V> PromiseLike<V> then(@Nullable ThenOnFulfilledCallbackFn<? super JsResolvedTable, ? extends V> onFulfilled,
    // @Nullable ThenOnRejectedCallbackFn<? extends V> onRejected);

    TableReference tableReference();

    // region Calls from Java's TableOperations

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
                .setInitial(options.initial != null ? options.initial : false)
                .setIncremental(options.incremental != null ? options.incremental : false)
                .setHistory(options.history != null ? options.history : false)
                .addAllStampColumns(
                        options.stampColumns != null ? options.stampColumns.asList() : Collections.emptyList())));
    }

    // omitting sortDescending, sort objects have this for us
    // TODO support simple struct for "expected names"?
    @JsMethod
    default JsPendingTable sort(ReadonlyArray<Sort> sorts) {
        Ticket ticket = getConnection().getTickets().newExportTicket();

        return call(ticket, BatchTableRequest.Operation.newBuilder().setSort(SortTableRequest.newBuilder()
                .setSourceId(TableReference.newBuilder()
                        .setTicket(typedTicket().getTicket())
                        .build())
                .setResultId(ticket)
                .addAllSorts(sorts.asList().stream().map(Sort::makeDescriptor).toList())));
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
    default JsPendingTable whereIn(JsTableOperations rightTable, ReadonlyArray<ColumnOrName> columnsToMatch) {
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
    default JsPendingTable whereNotIn(JsTableOperations rightTable, ReadonlyArray<ColumnOrName> columnsToMatch) {
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

    @TsUnion
    @JsType(name = "?", namespace = JsPackage.GLOBAL, isNative = true)
    public interface ColumnOrName {
        @JsOverlay
        static Function<ColumnOrName, String> COLUMN_NAME = c -> c.isString() ? c.asString() : c.asColumn().getName();

        @JsOverlay
        static ColumnOrName of(@DoNotAutobox Object value) {
            return Js.cast(value);
        }

        @JsOverlay
        default boolean isString() {
            return (Object) this instanceof String;
        }

        @JsOverlay
        default boolean isColumn() {
            return (Object) this instanceof Column;
        }

        @JsOverlay
        @TsUnionMember
        default String asString() {
            return Js.asString(this);
        }

        @JsOverlay
        @TsUnionMember
        default Column asColumn() {
            return Js.cast(this);
        }

    }

    // @JsMethod
    // JsTableOperations groupBy(JsArray<ColumnOrName> columns);

    // TODO re-model aggs into a new options object
    // @JsMethod
    // JsTableOperations aggAllBy(JsTotalsTableConfig config);

    // TODO single agg model, plus preserveEmpty boolean?
    // @JsMethod
    // JsTableOperations aggBy(JsTotalsTableConfig config);

    // TODO options
    // @JsMethod
    // JsTableOperations updateBy();
    //
    // @JsMethod
    // JsTableOperations selectDistinct(JsArray<ColumnOrName> names);
    //
    // @JsMethod
    // JsTableOperations countBy(ColumnOrName columName, JsArray<ColumnOrName> groupByColumns);
    //
    //
    // @JsMethod
    // JsTableOperations firstBy(JsArray<ColumnOrName> groupByColumns);
    //
    // @JsMethod
    // JsTableOperations lastBy(JsArray<ColumnOrName> groupByColumns);
    //
    // @JsMethod
    // JsTableOperations minBy(JsArray<ColumnOrName> groupByColumns);
    //
    // @JsMethod
    // JsTableOperations maxBy(JsArray<ColumnOrName> groupByColumns);
    //
    // @JsMethod
    // JsTableOperations sumBy(JsArray<ColumnOrName> groupByColumns);
    //
    // @JsMethod
    // JsTableOperations avgBy(JsArray<ColumnOrName> groupByColumns);
    //
    // @JsMethod
    // JsTableOperations medianBy(JsArray<ColumnOrName> groupByColumns);
    //
    // @JsMethod
    // JsTableOperations stdBy(JsArray<ColumnOrName> groupByColumns);
    //
    // @JsMethod
    // JsTableOperations varBy(JsArray<ColumnOrName> groupByColumns);
    //
    // @JsMethod
    // JsTableOperations absSumBy(JsArray<ColumnOrName> groupByColumns);
    //
    // @JsMethod
    // JsTableOperations wsumBy(ColumnOrName weightColumn, JsArray<ColumnOrName> groupByColumns);
    //
    // @JsMethod
    // JsTableOperations wavgBy(ColumnOrName weightColumn, JsArray<ColumnOrName> groupByColumns);

    @TsInterface
    @JsType(isNative = false, namespace = "dh")
    public static class UngroupOptions {
        /**
         * Indicates if the ungrouped table should allow disparate sized arrays filling shorter columns with null
         * values. If false, then all arrays must be the same length. Defaults to false.
         */
        @JsProperty
        @JsNullable
        public Boolean nullFill;

        /**
         * Specific columns to ungroup. If absent or empty, all columns in the table will be ungrouped.
         */
        @JsProperty
        @JsNullable
        public ReadonlyArray<ColumnOrName> columnsToUngroup;
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
                .setNullFill(options != null && options.nullFill != null ? options.nullFill : false)
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
    default JsPendingTable dropColumn(ReadonlyArray<ColumnOrName> columnsToDrop) {
        Ticket ticket = getConnection().getTickets().newExportTicket();

        return call(ticket, BatchTableRequest.Operation.newBuilder().setDropColumns(DropColumnsRequest.newBuilder()
                .setResultId(ticket)
                .setSourceId(TableReference.newBuilder()
                        .setTicket(typedTicket().getTicket()))
                .addAllColumnNames(columnsToNameList(columnsToDrop))));
    }

    private static List<String> columnsToNameList(ReadonlyArray<ColumnOrName> columnsToDrop) {
        return columnsToDrop.asList().stream().map(ColumnOrName.COLUMN_NAME).toList();
    }
    // endregion


    // region methods from Table
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

    // @JsMethod
    // JsTableOperations flatten();



    // endregion

    // TODO options
    // @JsMethod
    // JsTableOperations downsample();
}
