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
 * A partitioned table is a {@link Table} with one or more columns containing non-null, like-defined constituent tables,
 * optionally with "key" columns defined to allow {@link #partitionedTransform(PartitionedTable, BinaryOperator)} or
 * proxied joins with other like-keyed partitioned tables.
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
     * Get the {@link TableDefinition definition} shared by all constituent {@link Table tables}.
     *
     * @return The constituent definition
     */
    TableDefinition constituentDefinition();

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
     * same relative order as the underlying partitioned table and its constituents.
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
