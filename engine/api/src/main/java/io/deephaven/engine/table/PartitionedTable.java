package io.deephaven.engine.table;

import io.deephaven.api.TableOperations;
import io.deephaven.api.filter.Filter;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.engine.liveness.LivenessNode;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Function;

/**
 * <p>
 * Interface for working with partitioned tables.
 * <p>
 * A partitioned table is a {@link Table} with one or more columns containing non-{@code null}, like-defined constituent
 * tables, optionally with "key" columns defined to allow
 * {@link #partitionedTransform(PartitionedTable, BinaryOperator)} or proxied joins with other like-keyed partitioned
 * tables.
 */
public interface PartitionedTable extends LivenessNode, LogOutputAppendable {

    /**
     * Interface for proxies created by {@link #proxy()}.
     */
    interface Proxy extends TableOperations<Proxy, TableOperations> {

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
     * @return The underlying {@link Table partitioned table}
     */
    Table table();

    /**
     * Get the names of all "key" columns that are part of {@code table().getDefinition()}. If there are no key columns,
     * the result will be empty.
     *
     * @return The key column names
     */
    Set<String> keyColumnNames();

    /**
     * Get the name of the "constituent" column of {@link Table tables}.
     *
     * @return The constituent column name
     */
    String constituentColumnName();

    /**
     * Get a {@link TableDefinition definition} shared by or
     * {@link TableDefinition#checkMutualCompatibility(TableDefinition) mutually compatible} with all constituent
     * {@link Table tables}.
     *
     * @return The constituent definition
     */
    TableDefinition constituentDefinition();

    /**
     * <p>
     * Can the constituents of the underlying {@link #table() partitioned table} change?
     * <p>
     * This is completely unrelated to whether the constituents themselves are {@link Table#isRefreshing() refreshing},
     * or whether the underlying partitioned table is refreshing. Note that the underlying partitioned table
     * <em>must</em> be refreshing if it contains any refreshing constituents.
     * <p>
     * PartitionedTables that specify {@code constituentChangesPermitted() == true} must be guaranteed to never change
     * their constituents. Formally, it is expected that
     * {@code table().getColumnSource(constituentColumnName()).isImmutable() == true}, that {@code table()} will never
     * report additions, removals, or shifts, and that any modifications reported will not change values in the
     * constituent column.
     *
     *
     * @return Whether the constituents of the underlying partitioned table can change
     */
    boolean constituentChangesPermitted();

    /**
     * Same as {@code proxy(true, true)}.
     *
     * @return A proxy that allows {@link TableOperations table operations} to be applied to the constituent tables of
     *         this PartitionedTable
     * @see #proxy(boolean, boolean)
     */
    @FinalDefault
    default Proxy proxy() {
        return proxy(true, true);
    }

    /**
     * <p>
     * Make a proxy that allows {@link TableOperations table operations} to be applied to the constituent tables of this
     * PartitionedTable.
     * <p>
     * Each operation thus applied will produce a new PartitionedTable with the results as in
     * {@link #transform(Function)} or {@link #partitionedTransform(PartitionedTable, BinaryOperator)}, and return a new
     * proxy to that PartitionedTable.
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
    PartitionedTable filter(Collection<? extends Filter> filters);

    /**
     * <p>
     * Apply {@code transformer} to all constituent {@link Table tables}, and produce a new PartitionedTable containing
     * the results.
     * <p>
     * {@code transformer} must be stateless, safe for concurrent use, and able to return a valid result for an empty
     * input table.
     *
     * @param transformer The {@link Function} to apply to all constituent {@link Table tables}
     * @return The new PartitionedTable containing the resulting constituents
     */
    PartitionedTable transform(@NotNull Function<Table, Table> transformer);

    /**
     * <p>
     * Apply {@code transformer} to all constituent {@link Table tables} found in {@code this} and {@code other} with
     * the same key column values, and produce a new PartitionedTable containing the results.
     * <p>
     * {@code transformer} must be stateless, safe for concurrent use, and able to return a valid result for empty input
     * tables.
     *
     * @param other The other PartitionedTable to find constituents in
     * @param transformer The {@link Function} to apply to all pairs of constituent {@link Table tables}
     * @return The new PartitionedTable containing the resulting constituents
     */
    PartitionedTable partitionedTransform(
            @NotNull PartitionedTable other,
            @NotNull BinaryOperator<Table> transformer);
}
