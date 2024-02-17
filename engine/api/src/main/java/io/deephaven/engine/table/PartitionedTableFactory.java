/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table;

import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.ServiceLoader;

/**
 * Factory for producing Deephaven engine {@link PartitionedTable} instances.
 */
public class PartitionedTableFactory {

    /**
     * Creator interface for runtime-supplied implementation.
     */
    public interface Creator {

        /**
         * @see PartitionedTableFactory#of(Table, Collection, boolean, String, TableDefinition, boolean) Factory method
         *      that delegates to this method
         */
        PartitionedTable of(
                @NotNull Table table,
                @NotNull Collection<String> keyColumnNames,
                boolean uniqueKeys,
                @NotNull String constituentColumnName,
                @NotNull TableDefinition constituentDefinition,
                boolean constituentChangesPermitted);

        /**
         * @see PartitionedTableFactory#of(Table) Factory method that delegates to this method
         */
        PartitionedTable of(@NotNull Table table);

        /**
         * @see PartitionedTableFactory#ofTables(TableDefinition, Table...) Factory method that delegates to this method
         */
        PartitionedTable ofTables(
                @NotNull TableDefinition constituentDefinition,
                @NotNull Table... constituents);

        /**
         * @see PartitionedTableFactory#ofTables(Table...) Factory method that delegates to this method
         */
        PartitionedTable ofTables(
                @NotNull Table... constituents);
    }

    /**
     * Creator provider to supply the implementation at runtime.
     */
    @FunctionalInterface
    public interface CreatorProvider {
        Creator get();
    }

    private static final class PartitionedTableCreatorHolder {
        private static final Creator creator = ServiceLoader.load(CreatorProvider.class).iterator().next().get();
    }

    private static Creator partitionedTableCreator() {
        return PartitionedTableCreatorHolder.creator;
    }

    /**
     * Construct a {@link PartitionedTable}.
     *
     * @param table The "raw" {@link Table table} of {@link Table tables}. Should be {@link Table#isRefreshing()
     *        refreshing} if any constituents are.
     * @param keyColumnNames The "key" column names from {@code table}. Key columns are used in
     *        {@link PartitionedTable#transform transform} to validate the safety and correctness of join operations and
     *        in {@link PartitionedTable#partitionedTransform partitionedTransform} to correlate tables that should be
     *        transformed together. Passing an ordered set is highly recommended.
     * @param uniqueKeys Whether the keys (key column values for a row considered as a tuple) in {@code table} are
     *        guaranteed to be unique
     * @param constituentColumnName The "constituent" column name from {@code table}. The constituent column contains
     *        the underlying non-{@code null} {@link Table tables} that make up the result PartitionedTable.
     * @param constituentDefinition A {@link TableDefinition} expected to be
     *        {@link TableDefinition#checkMutualCompatibility(TableDefinition) mutually compatible} with all values in
     *        the "constituent" column of {@code table}
     * @param constituentChangesPermitted Whether {@code table} is permitted to report changes that impact the
     *        constituent column; ignored (and treated as {@code false}) if {@code !table.isRefreshing()}
     * @return A new PartitionedTable as described
     */
    public static PartitionedTable of(
            @NotNull final Table table,
            @NotNull final Collection<String> keyColumnNames,
            final boolean uniqueKeys,
            @NotNull final String constituentColumnName,
            @NotNull final TableDefinition constituentDefinition,
            final boolean constituentChangesPermitted) {
        return partitionedTableCreator().of(
                table,
                keyColumnNames,
                uniqueKeys,
                constituentColumnName,
                constituentDefinition,
                constituentChangesPermitted);
    }

    /**
     * Construct a {@link PartitionedTable} as in
     * {@link #of(Table, Collection, boolean, String, TableDefinition, boolean)}, inferring most parameters as follows:
     * <dl>
     * <dt>{@code keyColumnNames}</dt>
     * <dd>The names of all columns with a non-{@link Table} data type</dd>
     * <dt>{@code uniqueKeys}</dt>
     * <dd>{@code false}</dd>
     * <dt>{@code constituentColumnName}</dt>
     * <dd>The name of the first column with a {@link Table} data type</dd>
     * <dt>{@code constituentDefinition}</dt>
     * <dd>The {@link TableDefinition} of the value at the first cell in the constituent column. Consequently,
     * {@code table} must be non-empty.</dd>
     * <dt>{@code constituentChangesPermitted}</dt>
     * <dd>The value of {@code !table.isRefreshing()}</dd>
     * </dl>
     *
     * @param table The "raw" {@link Table table} of {@link Table tables}
     * @return A new PartitionedTable as described
     */
    public static PartitionedTable of(@NotNull final Table table) {
        return partitionedTableCreator().of(table);
    }

    /**
     * Construct a {@link Table} with a single column containing the non-{@code null} values in {@code constituents},
     * and then use that to construct a {@link PartitionedTable} as in
     * {@link #of(Table, Collection, boolean, String, TableDefinition, boolean)}, inferring most parameters as follows:
     * <dl>
     * <dt>{@code keyColumnNames}</dt>
     * <dd>An empty list</dd>
     * <dt>{@code uniqueKeys}</dt>
     * <dd>{@code false}</dd>
     * <dt>{@code constituentColumnName}</dt>
     * <dd>The single column containing non-{@code null} values from {@code constituents}</dd>
     * <dt>{@code constituentChangesPermitted}</dt>
     * <dd>{@code false}</dd>
     * </dl>
     *
     * @param constituentDefinition A {@link TableDefinition} expected to be
     *        {@link TableDefinition#checkMutualCompatibility(TableDefinition) mutually compatible} with all values in
     *        the "constituent" column of {@code table}
     * @param constituents The constituent tables to include. {@code null} values will be ignored.
     * @return A new PartitionedTable as described
     */
    public static PartitionedTable ofTables(
            @NotNull final TableDefinition constituentDefinition,
            @NotNull final Table... constituents) {
        return partitionedTableCreator().ofTables(constituentDefinition, constituents);
    }

    /**
     * Construct a {@link Table} with a single column containing the non-{@code null} values in {@code constituents},
     * and then use that to construct a PartitionedTable as in
     * {@link #of(Table, Collection, boolean, String, TableDefinition, boolean)}, inferring most parameters as follows:
     * <dl>
     * <dt>{@code keyColumnNames}</dt>
     * <dd>An empty list</dd>
     * <dt>{@code uniqueKeys}</dt>
     * <dd>{@code false}</dd>
     * <dt>{@code constituentColumnName}</dt>
     * <dd>The single column containing non-{@code null} values from {@code constituents}</dd>
     * <dt>{@code constituentDefinition}</dt>
     * <dd>The {@link TableDefinition} of the first non-{@code null} value in {@code constituents}</dd>
     * <dt>{@code constituentChangesPermitted}</dt>
     * <dd>{@code false}</dd>
     * </dl>
     *
     * @param constituents The constituent tables to include. {@code null} values will be ignored. At least one
     *        non-{@code null} constituent must be supplied.
     * @return A new PartitionedTable as described
     */
    public static PartitionedTable ofTables(@NotNull final Table... constituents) {
        return partitionedTableCreator().ofTables(constituents);
    }
}
