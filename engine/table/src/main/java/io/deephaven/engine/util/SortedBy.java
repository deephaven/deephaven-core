package io.deephaven.engine.util;

import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.table.Table;
import io.deephaven.util.annotations.ScriptApi;
import org.jetbrains.annotations.NotNull;

import java.util.*;

import static io.deephaven.api.agg.Aggregation.AggSortedFirst;
import static io.deephaven.api.agg.Aggregation.AggSortedLast;

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

    private static String[] resultColumns(@NotNull final Table input) {
        return resultColumns(input, Collections.emptyList());
    }

    private static String[] resultColumns(@NotNull final Table input, @NotNull Collection<String> groupByColumns) {
        if (groupByColumns.isEmpty()) {
            return input.getDefinition().getColumnNamesArray();
        }
        final Set<String> groupBySet = new HashSet<>(groupByColumns);
        return input.getDefinition().getColumnNames().stream()
                .filter(col -> !groupBySet.contains(col)).toArray(String[]::new);
    }

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
        return input.aggBy(AggSortedFirst(sortColumnName, resultColumns(input)));
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
        return input.aggBy(AggSortedFirst(Arrays.asList(sortColumnNames), resultColumns(input)));
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
        return input.aggBy(
                AggSortedFirst(sortColumnName, resultColumns(input, Arrays.asList(groupByColumns))),
                groupByColumns);
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
        return input.aggBy(
                AggSortedFirst(Arrays.asList(sortColumnNames), resultColumns(input, Arrays.asList(groupByColumns))),
                groupByColumns);
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
        return input.aggBy(
                AggSortedFirst(sortColumnName, resultColumns(input, groupByColumns)),
                groupByColumns.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
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
        return input.aggBy(
                AggSortedFirst(sortColumnNames, resultColumns(input, groupByColumns)),
                groupByColumns.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
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
        return input.aggBy(AggSortedLast(sortColumnName, resultColumns(input)));
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
        return input.aggBy(AggSortedLast(Arrays.asList(sortColumnNames), resultColumns(input)));
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
        return input.aggBy(
                AggSortedLast(sortColumnName, resultColumns(input, Arrays.asList(groupByColumns))),
                groupByColumns);
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
        return input.aggBy(
                AggSortedLast(Arrays.asList(sortColumnNames), resultColumns(input, Arrays.asList(groupByColumns))),
                groupByColumns);
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
        return input.aggBy(
                AggSortedLast(sortColumnName, resultColumns(input, groupByColumns)),
                groupByColumns.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
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
        return input.aggBy(
                AggSortedLast(sortColumnNames, resultColumns(input, groupByColumns)),
                groupByColumns.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
    }
}
