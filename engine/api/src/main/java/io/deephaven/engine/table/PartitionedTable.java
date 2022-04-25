package io.deephaven.engine.table;

import io.deephaven.api.TableOperations;
import io.deephaven.api.filter.Filter;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.engine.liveness.LivenessNode;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * <p>
 * Interface for working with partitioned tables.
 * <p>
 * A partitioned table is a {@link Table} with one or more columns containing non-null, like-defined constituent tables,
 * optionally with "key" columns defined to allow {@link #partitionedTransform(PartitionedTable, BiFunction)} or proxied
 * joins with other like-keyed partitioned tables.
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
     * <p>
     * Make a proxy that allows {@link TableOperations table operations} to be applied to the constituent tables of this
     * PartitionedTable.
     * <p>
     * Each operation thus applied will produce a new PartitionedTable with the results as in
     * {@link #transform(Function)} or {@link #partitionedTransform(PartitionedTable, BiFunction)}, and return a new
     * proxy to that PartitionedTable.
     *
     * @return A proxy that allows {@link TableOperations table operations} to be applied to the constituent tables of
     *         this PartitionedTable
     */
    Proxy proxy();

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
     * {@code transformer} must be stateless and safe for concurrent use.
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
     * {@code transformer} must be stateless and safe for concurrent use.
     *
     * @param other The other PartitionedTable to find constituents in
     * @param transformer The {@link Function} to apply to all pairs of constituent {@link Table tables}
     * @return The new PartitionedTable containing the resulting constituents
     */
    PartitionedTable partitionedTransform(
            @NotNull PartitionedTable other,
            @NotNull BiFunction<Table, Table, Table> transformer);
}
