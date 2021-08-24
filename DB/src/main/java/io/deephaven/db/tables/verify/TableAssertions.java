package io.deephaven.db.tables.verify;

import io.deephaven.db.tables.SortingOrder;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.utils.QueryPerformanceRecorder;
import io.deephaven.db.v2.*;
import io.deephaven.db.v2.sortcheck.SortCheck;
import io.deephaven.db.v2.sources.ColumnSource;
import org.jetbrains.annotations.NotNull;

/**
 * Assert properties of a table.
 *
 * The table assertions verify that a table meets certain properties. Presently, that the table is
 * add only and that it is sorted by a particular column. The desired property is verified on the
 * initial table, and if the table is refreshing then a listener is attached to ensure that the
 * property remains true.
 *
 * The returned table may have attributes set which allow the query engine to more efficiently
 * perform downstream operations.
 */
public class TableAssertions {
    private TableAssertions() {}

    /**
     * Asserts that the {@code table} is append-only. If its rows are ever modified or removed, the
     * query will crash.
     * <p>
     * This can be used to ensure the safety and stability of stateful operations.
     *
     * @param table The table to apply the assertion to
     * @return The provided {@code table}.
     */
    public static Table assertAppendOnly(@NotNull Table table) {
        return assertAppendOnly(null, table);
    }

    /**
     * Asserts that the {@code table} is append-only. If its rows are ever modified or removed, the
     * query will crash.
     * <p>
     * This can be used to ensure the safety and stability of stateful operations.
     *
     * @param description An optional description which will be included in the exception message if
     *        the assertion is violated.
     * @param table The table to apply the assertion to
     * @return The provided {@code table}.
     */
    public static Table assertAppendOnly(String description, @NotNull Table table) {
        // noinspection ConstantConditions
        if (table == null) {
            throw new IllegalArgumentException("The table cannot be null!");
        }

        if (!(table instanceof DynamicTable))
            return table;

        DynamicTable dynamicTable = (DynamicTable) table;
        if (!dynamicTable.isRefreshing())
            return table;

        return QueryPerformanceRecorder.withNuggetThrowing(
            "assertAppendOnly(" + (description == null ? "" : description) + ')',
            () -> {

                final DynamicTable result = new QueryTable(dynamicTable.getDefinition(),
                    dynamicTable.getIndex(), dynamicTable.getColumnSourceMap());
                final ShiftAwareListener listener =
                    new AppendOnlyAssertionInstrumentedListenerAdapter(description, dynamicTable,
                        result);
                dynamicTable.listenForUpdates(listener);

                return result;
            });
    }

    /**
     * Asserts that the {@code table} is sorted by the given column.
     *
     * This allows range filters to utilize binary search instead of a linear scan of the table for
     * the given column.
     *
     * @param table The table to apply the assertion to
     * @param column The column that the table is sorted by.
     * @param order Whether the column is ascending or descending.
     * @return The provided {@code table}.
     */
    public static Table assertSorted(@NotNull Table table, @NotNull final String column,
        SortingOrder order) {
        return assertSorted(null, table, column, order);
    }

    /**
     * Asserts that the {@code table} is sorted by the given column.
     *
     * This allows range filters to utilize binary search instead of a linear scan of the table for
     * the given column.
     *
     * @param description An optional description which will be included in the exception message if
     *        the assertion is violated.
     * @param table The table to apply the assertion to
     * @param column The column that the table is sorted by.
     * @param order Whether the column is ascending or descending.
     * @return The provided {@code table}.
     */
    public static Table assertSorted(String description, @NotNull Table table,
        @NotNull final String column, SortingOrder order) {
        // noinspection ConstantConditions
        if (table == null) {
            throw new IllegalArgumentException("The table cannot be null!");
        }

        // do the initial check
        final ColumnSource<?> columnSource = table.getColumnSource(column);
        SortedAssertionInstrumentedListenerAdapter.doCheckStatic(table.getIndex(), columnSource,
            SortCheck.make(columnSource.getChunkType(), order.isDescending()), description, column,
            order);


        if (!(table instanceof DynamicTable)) {
            SortedColumnsAttribute.setOrderForColumn(table, column, order);
            return table;
        }
        final DynamicTable dynamicTable = (DynamicTable) table;
        if (!dynamicTable.isRefreshing()) {
            SortedColumnsAttribute.setOrderForColumn(table, column, order);
            return table;
        }

        return QueryPerformanceRecorder.withNuggetThrowing(
            "assertSorted(" + (description == null ? "" : description) + ", " + column + ", "
                + order + ')',
            () -> {
                final DynamicTable result =
                    new QueryTable(dynamicTable.getIndex(), dynamicTable.getColumnSourceMap());
                final ShiftAwareListener listener = new SortedAssertionInstrumentedListenerAdapter(
                    description, dynamicTable, result, column, order);
                dynamicTable.listenForUpdates(listener);
                ((BaseTable) dynamicTable).copyAttributes(result, s -> true);
                SortedColumnsAttribute.setOrderForColumn(result, column, order);
                return result;
            });
    }

    /*
     * 
     * Some other things that might be nice:
     * 
     * assertIsNormalDecimal(Table, String... cols) assertPositive(Table, String... cols)
     * assertNegative(Table, String... cols) assertNull(Table, String... cols) assertNotNull(Table,
     * String... cols) assertFormula(Table, String... formulas)
     * 
     * assertColumnEqual(Table, String col1, String col2) assertColumnGreater(Table, String col1,
     * String col2) assertColumnLess(Table, String col1, String col2) assertColumnGeq(Table, String
     * col1, String col2) assertColumnLeq(Table, String col1, String col2)
     * 
     * assertColumnEqual(Table, double tolerance, String col1, String col2)
     * assertColumnGreater(Table, double tolerance, String col1, String col2)
     * assertColumnLess(Table, double tolerance, String col1, String col2) assertColumnGeq(Table,
     * double tolerance, String col1, String col2) assertColumnLeq(Table, double tolerance, String
     * col1, String col2)
     * 
     */

}
