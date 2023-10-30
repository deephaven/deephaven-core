/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table;

import io.deephaven.api.SortColumn;
import io.deephaven.api.TableOperations;
import io.deephaven.api.TableOperationsDefaults;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.util.ConcurrentMethod;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessNode;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;

/**
 * <p>
 * Interface for working with partitioned tables.
 * <p>
 * A partitioned table is a {@link Table} with one or more columns containing non-{@code null}, like-defined constituent
 * tables, optionally with "key" columns defined to allow
 * {@link #partitionedTransform(PartitionedTable, BinaryOperator)} or proxied joins with other like-keyed partitioned
 * tables.
 * <p>
 * Note that partitioned tables should {@link io.deephaven.engine.updategraph.NotificationQueue.Dependency depend} on
 * and {@link io.deephaven.engine.liveness.LivenessManager#manage(LivenessReferent) manage} their
 * {@link Table#isRefreshing() refreshing} constituents.
 */
public interface PartitionedTable extends LivenessNode, LogOutputAppendable {

    /**
     * Interface for proxies created by {@link #proxy()}.
     */
    interface Proxy extends TableOperationsDefaults<Proxy, TableOperations<?, ?>> {

        /**
         * Get the PartitionedTable instance underlying this proxy.
         *
         * @return The underlying PartitionedTable instance
         */
        PartitionedTable target();

        /**
         * @return Whether this Proxy ensures matching partition keys for implicit joins of partitioned tables
         * @see PartitionedTable#proxy(boolean, boolean)
         */
        boolean requiresMatchingKeys();

        /**
         * @return Whether this Proxy ensures non-overlapping join keys in constituent tables for proxied joins
         * @see PartitionedTable#proxy(boolean, boolean)
         */
        boolean sanityChecksJoins();
    }

    /**
     * Get the "raw" {@link Table partitioned table} underlying this PartitionedTable.
     *
     * <p>
     * The raw table can be converted back into a partitioned table using {@link PartitionedTableFactory#of(Table)} or
     * {@link PartitionedTableFactory#of(Table, Collection, boolean, String, TableDefinition, boolean)}.
     * </p>
     *
     * @return The underlying {@link Table partitioned table}
     */
    @ConcurrentMethod
    Table table();

    /**
     * Get the names of all "key" columns that are part of {@code table().getDefinition()}. If there are no key columns,
     * the result will be empty. This set is explicitly ordered.
     *
     * @return The key column names
     */
    @ConcurrentMethod
    Set<String> keyColumnNames();

    /**
     * <p>
     * Are the keys (key column values for a row considered as a tuple) in the underlying {@link #table() partitioned
     * table} unique?
     * <p>
     * If keys are unique, one can expect that {@code table().selectDistinct(keyColumnNames.toArray(String[]::new))} is
     * equivalent to {@code table().view(keyColumnNames.toArray(String[]::new))}.
     *
     * @return Whether the keys in the underlying partitioned table are unique
     */
    @ConcurrentMethod
    boolean uniqueKeys();

    /**
     * Get the name of the "constituent" column of {@link Table tables}.
     *
     * @return The constituent column name
     */
    @ConcurrentMethod
    String constituentColumnName();

    /**
     * Get a {@link TableDefinition definition} shared by or
     * {@link TableDefinition#checkMutualCompatibility(TableDefinition) mutually compatible} with all constituent
     * {@link Table tables}.
     *
     * @return The constituent definition
     */
    @ConcurrentMethod
    TableDefinition constituentDefinition();

    /**
     * <p>
     * Can the constituents of the underlying {@link #table() partitioned table} change?
     * <p>
     * This is completely unrelated to whether the constituents themselves are {@link Table#isRefreshing() refreshing},
     * or whether the underlying partitioned table is refreshing. Note that the underlying partitioned table
     * <em>must</em> be refreshing if it contains any refreshing constituents.
     * <p>
     * PartitionedTables that specify {@code constituentChangesPermitted() == false} must be guaranteed to never change
     * their constituents. Formally, it is expected that {@code table()} will never report additions, removals, or
     * shifts, and that any modifications reported will not change values in the constituent column (that is,
     * {@code table().getColumnSource(constituentColumnName())}).
     *
     * @return Whether the constituents of the underlying partitioned table can change
     */
    @ConcurrentMethod
    boolean constituentChangesPermitted();

    /**
     * Same as {@code proxy(true, true)}.
     *
     * @return A proxy that allows {@link TableOperations table operations} to be applied to the constituent tables of
     *         this PartitionedTable
     * @see #proxy(boolean, boolean)
     */
    @FinalDefault
    @ConcurrentMethod
    default Proxy proxy() {
        return proxy(true, true);
    }

    /**
     * <p>
     * Make a proxy that allows {@link TableOperations table operations} to be applied to the constituent tables of this
     * PartitionedTable.
     * <p>
     * Each operation thus applied will produce a new PartitionedTable with the results as in
     * {@link #transform(UnaryOperator)} or {@link #partitionedTransform(PartitionedTable, BinaryOperator)}, and return
     * a new proxy to that PartitionedTable.
     *
     * @param requireMatchingKeys Whether to ensure that both partitioned tables have all the same keys present when a
     *        proxied operation uses {@code this} and another {@link PartitionedTable} as inputs for a
     *        {@link #partitionedTransform(PartitionedTable, BinaryOperator) partitioned transform}
     * @param sanityCheckJoinOperations Whether to check that proxied join operations will only find a given join key in
     *        one constituent table for {@code this} and the {@link Table table} argument if it is also a
     *        {@link PartitionedTable.Proxy proxy}
     * @return A proxy that allows {@link TableOperations table operations} to be applied to the constituent tables of
     *         this PartitionedTable
     */
    @ConcurrentMethod
    Proxy proxy(boolean requireMatchingKeys, boolean sanityCheckJoinOperations);

    /**
     * Make a new {@link Table} that contains the rows from all the constituent tables of this PartitionedTable, in the
     * same relative order as the underlying partitioned table and its constituents. If constituent tables contain extra
     * columns not in the {@link #constituentDefinition() constituent definition}, those columns will be ignored. If
     * constituent tables are missing columns in the constituent definition, the corresponding output rows will be
     * {@code null}.
     *
     * @return A merged representation of the constituent tables
     */
    Table merge();

    /**
     * <p>
     * Make a new PartitionedTable from the result of applying {@code filters} to the underlying partitioned table.
     * <p>
     * {@code filters} must not reference the constituent column.
     *
     * @param filters The filters to apply. Must not reference the constituent column.
     * @return The filtered PartitionedTable
     */
    @ConcurrentMethod
    PartitionedTable filter(Collection<? extends Filter> filters);

    /**
     * <p>
     * Make a new PartitionedTable from the result of applying {@code sortColumns} to the underlying partitioned table.
     * <p>
     * {@code sortColumns} must not reference the constituent column.
     *
     * @param sortColumns The columns to sort by. Must not reference the constituent column.
     * @return The sorted PartitionedTable
     */
    @ConcurrentMethod
    PartitionedTable sort(Collection<SortColumn> sortColumns);

    /**
     * <p>
     * Apply {@code transformer} to all constituent {@link Table tables}, and produce a new PartitionedTable containing
     * the results.
     * <p>
     * This overload uses the {@link ExecutionContext#getContextToRecord enclosing ExecutionContext} and expects
     * {@code transformer} to produce {@link Table#isRefreshing() refreshing} results if and only if this
     * PartitionedTable's {@link #table() underlying table} is refreshing.
     * <p>
     *
     * @apiNote {@code transformer} must be stateless, safe for concurrent use, and able to return a valid result for an
     *          empty input table. It is required to install an ExecutionContext to access any
     *          QueryLibrary/QueryScope/QueryCompiler functionality from the {@code transformer}.
     *
     * @param transformer The {@link UnaryOperator} to apply to all constituent {@link Table tables}
     * @return The new PartitionedTable containing the resulting constituents
     * @throws IllegalStateException On instantiation or update if {@code !table().isRefreshing()} and
     *         {@code transformer} produces a refreshing result for any constituent
     */
    default PartitionedTable transform(@NotNull UnaryOperator<Table> transformer) {
        return transform(ExecutionContext.getContextToRecord(), transformer, table().isRefreshing());
    }

    /**
     * <p>
     * Apply {@code transformer} to all constituent {@link Table tables}, and produce a new PartitionedTable containing
     * the results. The {@code transformer} will be invoked in the provided ExecutionContext.
     * <p>
     * {@code transformer} must be stateless, safe for concurrent use, and able to return a valid result for an empty
     * input table.
     *
     * @param executionContext The ExecutionContext to use for the {@code transformer}
     * @param transformer The {@link UnaryOperator} to apply to all constituent {@link Table tables}
     * @param expectRefreshingResults Whether to expect that the results of applying {@code transformer} <em>may</em> be
     *        {@link Table#isRefreshing() refreshing}. If {@code true}, the resulting PartitionedTable will always be
     *        backed by a refreshing {@link #table() table}. This hint is important for transforms to static inputs that
     *        might produce refreshing output, in order to ensure correct liveness management; incorrectly specifying
     *        {@code false} will result in exceptions.
     * @return The new PartitionedTable containing the resulting constituents
     * @throws IllegalStateException On instantiation or update if
     *         {@code !table().isRefreshing() && !expectRefreshingResults} and {@code transformer} produces a refreshing
     *         result for any constituent
     */
    PartitionedTable transform(
            @Nullable ExecutionContext executionContext,
            @NotNull UnaryOperator<Table> transformer,
            boolean expectRefreshingResults);

    /**
     * <p>
     * Apply {@code transformer} to all constituent {@link Table tables} found in {@code this} and {@code other} with
     * the same key column values, and produce a new PartitionedTable containing the results.
     * <p>
     * Note that {@code other}'s key columns must match {@code this} PartitionedTable's key columns. Two matching
     * mechanisms are supported, and will be attempted in the order listed:
     * <ol>
     * <li>Match by column name. Both PartitionedTables must have all the same {@link #keyColumnNames() key column
     * names}. Like-named columns must have the same {@link ColumnSource#getType() data type} and
     * {@link ColumnSource#getComponentType() component type}.</li>
     * <li>Match by column order. Both PartitionedTables must have their matchable columns in the same order within
     * their {@link #keyColumnNames() key column names}. Like-positioned columns must have the same
     * {@link ColumnSource#getType() data type} and {@link ColumnSource#getComponentType() component type}.</li>
     * </ol>
     * <p>
     * This overload uses the {@link ExecutionContext#getContextToRecord enclosing ExecutionContext} and expects
     * {@code transformer} to produce {@link Table#isRefreshing() refreshing} results if and only if {@code this} or
     * {@code other} has a refreshing {@link #table() underlying table}.
     * <p>
     *
     * @apiNote {@code transformer} must be stateless, safe for concurrent use, and able to return a valid result for
     *          empty input tables. It is required to install an ExecutionContext to access any
     *          QueryLibrary/QueryScope/QueryCompiler functionality from the {@code transformer}.
     *
     * @param other The other PartitionedTable to find constituents in
     * @param transformer The {@link BinaryOperator} to apply to all pairs of constituent {@link Table tables}
     * @return The new PartitionedTable containing the resulting constituents
     * @throws IllegalStateException On instantiation or update if
     *         {@code !table().isRefreshing() && !other.table().isRefreshing()} and {@code transformer} produces a
     *         refreshing result for any constituent
     */
    default PartitionedTable partitionedTransform(
            @NotNull PartitionedTable other,
            @NotNull BinaryOperator<Table> transformer) {
        return partitionedTransform(other, ExecutionContext.getContextToRecord(), transformer,
                table().isRefreshing() || other.table().isRefreshing());
    }

    /**
     * <p>
     * Apply {@code transformer} to all constituent {@link Table tables} found in {@code this} and {@code other} with
     * the same key column values, and produce a new PartitionedTable containing the results. The {@code transformer}
     * will be invoked in the provided ExecutionContext.
     * <p>
     * Note that {@code other}'s key columns must match {@code this} PartitionedTable's key columns. Two matching
     * mechanisms are supported, and will be attempted in the order listed:
     * <ol>
     * <li>Match by column name. Both PartitionedTables must have all the same {@link #keyColumnNames() key column
     * names}. Like-named columns must have the same {@link ColumnSource#getType() data type} and
     * {@link ColumnSource#getComponentType() component type}.</li>
     * <li>Match by column order. Both PartitionedTables must have their matchable columns in the same order within
     * their {@link #keyColumnNames() key column names}. Like-positioned columns must have the same
     * {@link ColumnSource#getType() data type} and {@link ColumnSource#getComponentType() component type}.</li>
     * </ol>
     * <p>
     * {@code transformer} must be stateless, safe for concurrent use, and able to return a valid result for empty input
     * tables. It is required to install an ExecutionContext to access any QueryLibrary/QueryScope/QueryCompiler
     * functionality from the {@code transformer}.
     *
     * @param other The other PartitionedTable to find constituents in
     * @param executionContext The ExecutionContext to use for the {@code transformer}
     * @param transformer The {@link BinaryOperator} to apply to all pairs of constituent {@link Table tables}
     * @param expectRefreshingResults Whether to expect that the results of applying {@code transformer} <em>may</em> be
     *        {@link Table#isRefreshing() refreshing}. If {@code true}, the resulting PartitionedTable will always be
     *        backed by a refreshing {@link #table() table}. This hint is important for transforms to static inputs that
     *        might produce refreshing output, in order to ensure correct liveness management; incorrectly specifying
     *        {@code false} will result in exceptions.
     * @return The new PartitionedTable containing the resulting constituents
     * @throws IllegalStateException On instantiation or update if
     *         {@code !table().isRefreshing() && !other.table().isRefreshing() && !expectRefreshingResults} and
     *         {@code transformer} produces a refreshing result for any constituent
     */
    PartitionedTable partitionedTransform(
            @NotNull PartitionedTable other,
            @Nullable ExecutionContext executionContext,
            @NotNull BinaryOperator<Table> transformer,
            boolean expectRefreshingResults);

    /**
     * <p>
     * Get a single {@link Table constituent} by its corresponding key column values.
     * <p>
     * The {@code keyColumnValues} can be thought of as a tuple constraining the values for the corresponding key
     * columns for the result row. If there are no matching rows, the result is {@code null}. If there are multiple
     * matching rows, an {@link UnsupportedOperationException} is thrown.
     * <p>
     * The result will be {@link io.deephaven.engine.liveness.LivenessManager#manage(LivenessReferent) managed} by the
     * enclosing {@link io.deephaven.engine.liveness.LivenessScopeStack#peek() liveness scope}.
     * <p>
     * Note that if {@link #constituentChangesPermitted()}, this method may return different results if invoked multiple
     * times.
     *
     * @param keyColumnValues Ordered, boxed values for the key columns in the same order as {@link #keyColumnNames()}
     * @throws IllegalArgumentException If {@code keyColumnValues.length != keyColumnNames().size()}
     * @throws UnsupportedOperationException If multiple matching rows for the {@code keyColumnValues} were found
     * @return The {@link Table constituent} at the single row in {@link #table()} matching the {@code keyColumnValues},
     *         or {@code null} if no matches were found
     */
    @ConcurrentMethod
    Table constituentFor(@NotNull Object... keyColumnValues);

    /**
     * Get all the current {@link Table constituents}.
     * <p>
     * The results will be {@link io.deephaven.engine.liveness.LivenessManager#manage(LivenessReferent) managed} by the
     * enclosing {@link io.deephaven.engine.liveness.LivenessScopeStack#peek() liveness scope}.
     * <p>
     * Note that if {@link #constituentChangesPermitted()}, this method may return different results if invoked multiple
     * times.
     *
     * @return An array of all current {@link Table constituents}
     */
    @ConcurrentMethod
    Table[] constituents();
}
