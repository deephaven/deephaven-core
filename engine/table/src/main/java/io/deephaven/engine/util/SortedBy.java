package io.deephaven.engine.util;

import io.deephaven.engine.table.Table;
import io.deephaven.util.annotations.ScriptApi;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;

import static io.deephaven.api.agg.spec.AggSpec.sortedFirst;
import static io.deephaven.api.agg.spec.AggSpec.sortedLast;

/**
 * SortedBy operations sort the values in each of the buckets according to a specified column. The sortedFirstBy returns
 * the row with the lowest value and sortedLastBy returns the row with the greatest value.
 */
@ScriptApi
public class SortedBy {
    /**
     * Static use only.
     */
    private SortedBy() {}

    /**
     * Return a new table with a single row, containing the lowest value of sortColumnName.
     *
     * @param input the input table
     * @param sortColumnName the name of the column to sort by
     *
     * @return a new table containing the row with the lowest value of the sort column
     */
    @NotNull
    public static Table sortedFirstBy(@NotNull Table input, @NotNull String sortColumnName) {
        return input.aggAllBy(sortedFirst(sortColumnName));
    }

    /**
     * Return a new table with a single row, containing the lowest value of sortColumnName.
     *
     * @param input the input table
     * @param sortColumnNames the names of the column to sort by
     *
     * @return a new table containing the row with the lowest value of the sort columns
     */
    @NotNull
    public static Table sortedFirstBy(@NotNull Table input, @NotNull String[] sortColumnNames) {
        return input.aggAllBy(sortedFirst(sortColumnNames));
    }

    /**
     * Return an aggregated table with the lowest value of sortColumnName for each grouping key.
     *
     * @param input the input table
     * @param sortColumnName the name of the column to sort by
     * @param groupByColumns the columns to group by
     *
     * @return a new table containing the rows with the lowest value of the sort column for each grouping key
     */
    @NotNull
    public static Table sortedFirstBy(@NotNull Table input, @NotNull String sortColumnName,
            @NotNull String... groupByColumns) {
        return input.aggAllBy(sortedFirst(sortColumnName), groupByColumns);
    }

    /**
     * Return an aggregated table with the lowest value of the sort columns for each grouping key.
     *
     * @param input the input table
     * @param sortColumnNames the names of the column to sort by
     * @param groupByColumns the columns to group by
     *
     * @return a new table containing the rows with the lowest value of the sort columns for each grouping key
     */
    @NotNull
    public static Table sortedFirstBy(@NotNull Table input, @NotNull String[] sortColumnNames,
            @NotNull String... groupByColumns) {
        return input.aggAllBy(sortedFirst(sortColumnNames), groupByColumns);
    }

    /**
     * Return an aggregated table with the lowest value of sortColumnName for each grouping key.
     *
     * @param input the input table
     * @param sortColumnName the name of the column to sort by
     * @param groupByColumns the columns to group by
     *
     * @return a new table containing the rows with the lowest value of the sort column for each grouping key
     */
    @NotNull
    public static Table sortedFirstBy(@NotNull Table input, @NotNull String sortColumnName,
            @NotNull Collection<String> groupByColumns) {
        return input.aggAllBy(sortedFirst(sortColumnName), groupByColumns);
    }

    /**
     * Return an aggregated table with the lowest value of the sort columns for each grouping key.
     *
     * @param input the input table
     * @param sortColumnNames the names of the column to sort by
     * @param groupByColumns the columns to group by
     *
     * @return a new table containing the rows with the lowest value of the sort columns for each grouping key
     */
    @NotNull
    public static Table sortedFirstBy(@NotNull Table input, @NotNull Collection<String> sortColumnNames,
            @NotNull Collection<String> groupByColumns) {
        return input.aggAllBy(sortedFirst(sortColumnNames), groupByColumns);
    }

    /**
     * Return a new table with a single row, containing the greatest value of sortColumnName.
     *
     * @param input the input table
     * @param sortColumnName the name of the column to sort by
     *
     * @return a new table containing the row with the greatest value of the sort column
     */
    @NotNull
    public static Table sortedLastBy(@NotNull Table input, @NotNull String sortColumnName) {
        return input.aggAllBy(sortedLast(sortColumnName));
    }

    /**
     * Return a new table with a single row, containing the greatest value of sortColumnName.
     *
     * @param input the input table
     * @param sortColumnNames the name of the columns to sort by
     *
     * @return a new table containing the row with the greatest value of the sort column
     */
    @NotNull
    public static Table sortedLastBy(@NotNull Table input, @NotNull String[] sortColumnNames) {
        return input.aggAllBy(sortedLast(sortColumnNames));
    }

    /**
     * Return an aggregated table with the greatest value of the sort column for each grouping key.
     *
     * @param input the input table
     * @param sortColumnName the name of the column to sort by
     * @param groupByColumns the columns to group by
     *
     * @return a new table containing the rows with the greatest value of the sort column for each grouping key
     */
    @NotNull
    public static Table sortedLastBy(@NotNull Table input, @NotNull String sortColumnName,
            @NotNull String... groupByColumns) {
        return input.aggAllBy(sortedLast(sortColumnName), groupByColumns);

    }

    /**
     * Return an aggregated table with the greatest value of the sort columns for each grouping key.
     *
     * @param input the input table
     * @param sortColumnNames the names of the column to sort by
     * @param groupByColumns the columns to group by
     *
     * @return a new table containing the rows with the greatest value of the sort columns for each grouping key
     */
    @NotNull
    public static Table sortedLastBy(@NotNull Table input, @NotNull String[] sortColumnNames,
            @NotNull String... groupByColumns) {
        return input.aggAllBy(sortedLast(sortColumnNames), groupByColumns);
    }

    /**
     * Return an aggregated table with the greatest value of the sort column for each grouping key.
     *
     * @param input the input table
     * @param sortColumnName the name of the column to sort by
     * @param groupByColumns the columns to group by
     *
     * @return a new table containing the rows with the greatest value of the sort column for each grouping key
     */
    @NotNull
    public static Table sortedLastBy(@NotNull Table input, @NotNull String sortColumnName,
            @NotNull Collection<String> groupByColumns) {
        return input.aggAllBy(sortedLast(sortColumnName), groupByColumns);
    }

    /**
     * Return an aggregated table with the greatest value of the sort columns for each grouping key.
     *
     * @param input the input table
     * @param sortColumnNames the names of the column to sort by
     * @param groupByColumns the columns to group by
     *
     * @return a new table containing the rows with the greatest value of the sort columns for each grouping key
     */
    @NotNull
    public static Table sortedLastBy(@NotNull Table input, @NotNull Collection<String> sortColumnNames,
            @NotNull Collection<String> groupByColumns) {
        return input.aggAllBy(sortedLast(sortColumnNames), groupByColumns);
    }
}
