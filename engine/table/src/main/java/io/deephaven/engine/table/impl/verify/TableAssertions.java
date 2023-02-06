/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.verify;

import io.deephaven.engine.table.TableUpdateListener;
import io.deephaven.engine.table.impl.SortingOrder;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.sortcheck.SortCheck;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

/**
 * Assert properties of a table.
 * <p>
 * The table assertions verify that a table meets certain properties. Presently, that the table is add only and that it
 * is sorted by a particular column. The desired property is verified on the initial table, and if the table is
 * refreshing then a listener is attached to ensure that the property remains true.
 * <p>
 * The returned table may have attributes set which allow the query engine to more efficiently perform downstream
 * operations.
 */
public class TableAssertions {
    private TableAssertions() {}

    /**
     * Asserts that the {@code table} is append-only. If its rows are ever modified or removed, the query will crash.
     * <p>
     * This can be used to ensure the safety and stability of stateful operations.
     *
     * @param table The table to apply the assertion to
     * @return {@code table}, or a copy with the appropriate assertion applied
     */
    public static Table assertAppendOnly(@NotNull Table table) {
        return assertAppendOnly(null, table);
    }

    /**
     * Asserts that the {@code table} is append-only. If its rows are ever modified or removed, the query will crash.
     * <p>
     * This can be used to ensure the safety and stability of stateful operations.
     *
     * @param description An optional description which will be included in the exception message if the assertion is
     *        violated.
     * @param table The table to apply the assertion to
     * @return {@code table}, or a copy with the appropriate assertion applied
     */
    public static Table assertAppendOnly(String description, @NotNull Table table) {
        // noinspection ConstantConditions
        if (table == null) {
            throw new IllegalArgumentException("The table cannot be null!");
        }

        if (!table.isRefreshing()) {
            return table;
        }

        final QueryTable coalesced = (QueryTable) table.coalesce();
        return QueryPerformanceRecorder.withNuggetThrowing(
                "assertAppendOnly(" + (description == null ? "" : description) + ')',
                () -> {

                    final QueryTable result = new QueryTable(coalesced.getDefinition(), coalesced.getRowSet(),
                            coalesced.getColumnSourceMap());
                    final TableUpdateListener listener =
                            new AppendOnlyAssertionInstrumentedListenerAdapter(description, coalesced, result);
                    coalesced.addUpdateListener(listener);

                    return result;
                });
    }

    /**
     * Asserts that the {@code table} is sorted by the given column.
     * <p>
     * This allows range filters to utilize binary search instead of a linear scan of the table for the given column.
     *
     * @param table The table to apply the assertion to
     * @param column The column that the table is sorted by.
     * @param order Whether the column is ascending or descending.
     * @return {@code table}, or a copy with the appropriate assertion applied
     */
    public static Table assertSorted(@NotNull Table table, @NotNull final String column, SortingOrder order) {
        return assertSorted(null, table, column, order);
    }

    /**
     * Asserts that the {@code table} is sorted by the given column.
     * <p>
     * This allows range filters to utilize binary search instead of a linear scan of the table for the given column.
     *
     * @param description An optional description which will be included in the exception message if the assertion is
     *        violated.
     * @param table The table to apply the assertion to
     * @param column The column that the table is sorted by.
     * @param order Whether the column is ascending or descending.
     * @return {@code table}, or a copy with the appropriate assertion applied
     */
    public static Table assertSorted(String description, @NotNull Table table, @NotNull final String column,
            SortingOrder order) {
        // noinspection ConstantConditions
        if (table == null) {
            throw new IllegalArgumentException("The table cannot be null!");
        }

        // do the initial check
        final ColumnSource<?> columnSource = table.getColumnSource(column);
        SortedAssertionInstrumentedListenerAdapter.doCheckStatic(table.getRowSet(), columnSource,
                SortCheck.make(columnSource.getChunkType(), order.isDescending()), description, column, order);

        if (!table.isRefreshing()) {
            return SortedColumnsAttribute.withOrderForColumn(table, column, order);
        }

        final QueryTable coalesced = (QueryTable) table.coalesce();
        return QueryPerformanceRecorder.withNuggetThrowing(
                "assertSorted(" + (description == null ? "" : description) + ", " + column + ", " + order + ')',
                () -> {
                    final QueryTable result =
                            new QueryTable(coalesced.getRowSet(), coalesced.getColumnSourceMap());
                    final TableUpdateListener listener = new SortedAssertionInstrumentedListenerAdapter(description,
                            coalesced, result, column, order);
                    coalesced.addUpdateListener(listener);
                    ((BaseTable<?>) table).copyAttributes(result, s -> true);
                    SortedColumnsAttribute.setOrderForColumn(result, column, order);
                    return result;
                });
    }

    /*
     * 
     * Some other things that might be nice:
     * 
     * assertIsNormalDecimal(Table, String... cols) assertPositive(Table, String... cols) assertNegative(Table,
     * String... cols) assertNull(Table, String... cols) assertNotNull(Table, String... cols) assertFormula(Table,
     * String... formulas)
     * 
     * assertColumnEqual(Table, String col1, String col2) assertColumnGreater(Table, String col1, String col2)
     * assertColumnLess(Table, String col1, String col2) assertColumnGeq(Table, String col1, String col2)
     * assertColumnLeq(Table, String col1, String col2)
     * 
     * assertColumnEqual(Table, double tolerance, String col1, String col2) assertColumnGreater(Table, double tolerance,
     * String col1, String col2) assertColumnLess(Table, double tolerance, String col1, String col2)
     * assertColumnGeq(Table, double tolerance, String col1, String col2) assertColumnLeq(Table, double tolerance,
     * String col1, String col2)
     * 
     */

}
