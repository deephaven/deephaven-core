package io.deephaven.engine.table;

import org.jetbrains.annotations.NotNull;

import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Function;

/**
 * Factory for producing Deephaven engine {@link PartitionedTable} instances.
 */
public class PartitionedTableFactory {

    /**
     * Creator interface for runtime-supplied implementation.
     */
    public interface Creator {

        /**
         * @see PartitionedTableFactory#of(Table, Set, String, TableDefinition) Factory method that delegates to this
         *      method
         */
        PartitionedTable of(
                @NotNull Table table,
                @NotNull Set<String> keyColumnNames,
                @NotNull String constituentColumnName,
                @NotNull TableDefinition constituentDefinition);

        /**
         * @see PartitionedTableFactory#of(Table) Factory method that delegates to this method
         */
        PartitionedTable of(@NotNull Table table);
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
     * Construct a PartitionedTable.
     *
     * @param table The "raw" {@link Table table} of {@link Table tables}. Should be {@link Table#isRefreshing()
     *        refreshing} if any constituents are.
     * @param keyColumnNames The "key" column names from {@code table}. Key columns are used in
     *        {@link PartitionedTable#transform(Function)} to validate the safety and correctness of join operations and
     *        in {@link PartitionedTable#partitionedTransform(PartitionedTable, BinaryOperator)} to correlate tables
     *        that should be transformed together.
     * @param constituentColumnName The "constituent" column name from {@code table}. The constituent column contains
     *        the underlying {@link Table tables} that make up this PartitionedTable.
     * @param constituentDefinition The {@link TableDefinition} expected for all rows in the "constituent" column of
     *        {@code table}
     */
    public static PartitionedTable of(
            @NotNull final Table table,
            @NotNull final Set<String> keyColumnNames,
            @NotNull final String constituentColumnName,
            @NotNull final TableDefinition constituentDefinition) {
        return partitionedTableCreator().of(table, keyColumnNames, constituentColumnName, constituentDefinition);
    }

    /**
     * Construct a PartitionedTable as in {@link #of(Table, Set, String, TableDefinition)}, inferring most parameters as
     * follows:
     * <dl>
     * <dt>{@code keyColumnNames}</dt>
     * <dd>The names of all columns with a non-{@link Table} data type</dd>
     * <dt>{@code constituentColumnName}</dt>
     * <dd>The name of the first column with a {@link Table} data type</dd>
     * <dt>{@code constituentDefinition}</dt>
     * <dd>The {@link TableDefinition} of the value at the first cell in the constituent column. {@code table} must be
     * non-empty, and the first row of its constituent column must contain a non-{@code null} table value.</dd>
     * </dl>
     *
     * @param table The "raw" {@link Table table} of {@link Table tables}
     */
    public static PartitionedTable of(@NotNull final Table table) {
        return partitionedTableCreator().of(table);
    }
}
