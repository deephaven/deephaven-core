/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.util;

import com.google.common.collect.Streams;
import io.deephaven.api.Selectable;
import io.deephaven.api.TableOperationsDefaults;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.CrossJoinHelper;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.select.MatchPairFactory;
import io.deephaven.engine.table.impl.select.NullSelectColumn;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.select.SourceColumn;
import io.deephaven.engine.table.impl.sources.NullValueColumnSource;
import io.deephaven.util.annotations.ScriptApi;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Provides static methods to perform SQL-style left outer and full outer joins.
 */
public class OuterJoinTools {

    /**
     * Returns a table that has one column for each of table1 columns, and one column corresponding to each of table2
     * columns listed in the columns to add (or all the columns whose names don't overlap with the name of a column from
     * table1 if the columnsToAdd is length zero). The returned table will have one row for each matching set of keys
     * between the first and second tables, plus one row for any first table key set that doesn't match the second table
     * and one row for each key set from the second table that doesn't match the first table. Columns from either table
     * for which there was no match in the other table will have null values. Note that this method will cause tick
     * expansion with ticking tables.
     * <p>
     *
     * @param table1 input table
     * @param table2 input table
     * @param columnsToMatch match criteria
     * @param columnsToAdd columns to add
     * @return a table that has one column for each of table1's columns, and one column corresponding to each of
     *         table2's columns listed in the columns to add (or all the columns whose names don't overlap with the name
     *         of a column from table1 if the columnsToAdd is length zero). The returned table will have one row for
     *         each matching set of keys between the first and second tables, plus one row for any first table key set
     *         that doesn't match the second table and one row for each key set from the second table that doesn't match
     *         the first table. Columns from either table for which there was no match in the other table will have null
     *         values.
     */
    @ScriptApi
    public static Table fullOuterJoin(
            @NotNull final Table table1,
            @NotNull final Table table2,
            @NotNull final MatchPair[] columnsToMatch,
            @NotNull final MatchPair[] columnsToAdd) {
        // perform the leftOuterJoin; it's missing right-side only rows
        final Table leftTable = leftOuterJoin(table1, table2, columnsToMatch, columnsToAdd);

        // find a sentinel column name to use to identify right-side only rows
        int numAttempts = 0;
        String sentinelColumnName;
        final Map<String, ? extends ColumnSource<?>> resultColumns = leftTable.getColumnSourceMap();
        do {
            sentinelColumnName = "__sentinel_" + (numAttempts++) + "__";
        } while (resultColumns.containsKey(sentinelColumnName));

        // handle the simpler scenario where no conflicts may exist otherwise leftOuterJoin would have failed
        if (columnsToAdd.length == 0) {
            final MatchPair[] matchPairs = Arrays.stream(columnsToMatch)
                    .map(mp -> new MatchPair(mp.rightColumn(), mp.leftColumn()))
                    .toArray(MatchPair[]::new);

            final Table leftWithSentinel = table1.coalesce().updateView(sentinelColumnName + " = true");

            // we must use leftOuterJoin again as there may be multiple matches per group
            final Table rightTable = CrossJoinHelper.leftOuterJoin(
                    (QueryTable) table2.coalesce(),
                    (QueryTable) leftWithSentinel,
                    matchPairs,
                    createColumnsToAdd(leftWithSentinel, matchPairs, MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY),
                    CrossJoinHelper.DEFAULT_NUM_RIGHT_BITS_TO_RESERVE)
                    .where(sentinelColumnName + " == null")
                    .dropColumns(sentinelColumnName);

            return TableTools.merge(leftTable, rightTable);
        }

        // only need match columns from the left; rename and drop remaining to avoid column name conflicts
        final SelectColumn[] leftColumns = Streams.concat(
                Arrays.stream(columnsToMatch).map(mp -> new SourceColumn(mp.leftColumn(), mp.rightColumn())),
                List.of(SelectColumn.of(Selectable.parse(sentinelColumnName + " = true"))).stream())
                .toArray(SelectColumn[]::new);

        // we will modify the join to use exact matches
        final MatchPair[] matchColumns = Arrays.stream(columnsToMatch)
                .map(mp -> new MatchPair(mp.rightColumn(), mp.rightColumn()))
                .toArray(MatchPair[]::new);

        // prepare filter for columnsToAdd
        final SelectColumn[] rightColumns = Streams.concat(
                Arrays.stream(columnsToAdd).map(mp -> new SourceColumn(mp.rightColumn(), mp.leftColumn())),
                table1.getColumnSourceMap().entrySet().stream().map(entry -> new NullSelectColumn<>(
                        entry.getValue().getType(), entry.getValue().getComponentType(), entry.getKey())))
                .toArray(SelectColumn[]::new);

        // we must use leftOuterJoin again as there may be multiple matches per group
        final Table rightTable = CrossJoinHelper.leftOuterJoin(
                (QueryTable) table2.coalesce(),
                (QueryTable) table1.coalesce().view(leftColumns),
                matchColumns,
                new MatchPair[] {new MatchPair(sentinelColumnName, sentinelColumnName)},
                CrossJoinHelper.DEFAULT_NUM_RIGHT_BITS_TO_RESERVE)
                .where(sentinelColumnName + " == null")
                .view(rightColumns);

        return TableTools.merge(leftTable, rightTable);
    }

    private static MatchPair[] createColumnsToAdd(@NotNull final Table rightTable,
            @NotNull final MatchPair[] columnsToMatch,
            @NotNull final MatchPair[] columnsToAdd) {
        if (columnsToAdd.length > 0) {
            return columnsToAdd;
        }

        final Set<String> matchColumns = Arrays.stream(columnsToMatch)
                .map(MatchPair::leftColumn)
                .collect(Collectors.toCollection(HashSet::new));
        return rightTable.getDefinition().getColumnStream().map(ColumnDefinition::getName)
                .filter((name) -> !matchColumns.contains(name))
                .map(name -> new MatchPair(name, name))
                .toArray(MatchPair[]::new);
    }

    /**
     * Returns a table that has one column for each of the left table columns, and one column corresponding to each of
     * the right table columns listed in the columns to add (or all the columns whose names don't overlap with the name
     * of a column from the source table if the columnsToAdd is length zero). The returned table will have one row for
     * each matching set of keys between the left table and right table plus one row for any left table key set that
     * doesn't match the right table. Columns from the right table for which there was no match will have null values.
     * Note that this method will cause tick expansion with ticking tables.
     * <p>
     *
     * @param leftTable input table
     * @param rightTable input table
     * @param columnsToMatch match criteria
     * @param columnsToAdd columns to add
     * @return a table that has one column for each of the left table columns, and one column corresponding to each
     *         column listed in columnsToAdd. If columnsToAdd.length==0 one column corresponding to each column of the
     *         right table columns whose names don't overlap with the name of a column from the left table is added. The
     *         returned table will have one row for each matching set of keys between the left table and right table
     *         plus one row for any left table key set that doesn't match the right table. Columns from the right table
     *         for which there was no match will have null values.
     */
    @ScriptApi
    public static Table leftOuterJoin(
            @NotNull final Table leftTable,
            @NotNull final Table rightTable,
            @NotNull final MatchPair[] columnsToMatch,
            @NotNull final MatchPair[] columnsToAdd) {
        final MatchPair[] useColumnsToAdd = createColumnsToAdd(rightTable, columnsToMatch, columnsToAdd);
        return CrossJoinHelper.leftOuterJoin(
                (QueryTable) leftTable.coalesce(),
                (QueryTable) rightTable.coalesce(),
                columnsToMatch,
                useColumnsToAdd,
                CrossJoinHelper.DEFAULT_NUM_RIGHT_BITS_TO_RESERVE);
    }

    /**
     * Returns a table that has one column for each of the left table columns, and one column corresponding to each of
     * the right table columns listed in the columns to add (or all the columns whose names don't overlap with the name
     * of a column from the source table if the columnsToAdd is length zero). The returned table will have one row for
     * each matching set of keys between the left table and right table plus one row for any left table key set that
     * doesn't match the right table. Columns from the right table for which there was no match will have null values.
     * Note that this method will cause tick expansion with ticking tables.
     * <p>
     *
     * @param leftTable input table
     * @param rightTable input table
     * @param columnsToMatch match criteria
     * @param columnsToAdd columns to add
     * @return a table that has one column for each of the left table columns, and one column corresponding to each
     *         column listed in columnsToAdd. If columnsToAdd.length==0 one column corresponding to each column of the
     *         right table columns whose names don't overlap with the name of a column from the left table is added. The
     *         returned table will have one row for each matching set of keys between the left table and right table
     *         plus one row for any left table key set that doesn't match the right table. Columns from the right table
     *         for which there was no match will have null values.
     */
    @ScriptApi
    public static Table leftOuterJoin(
            @NotNull final Table leftTable,
            @NotNull final Table rightTable,
            @NotNull final Collection<String> columnsToMatch,
            @NotNull final Collection<String> columnsToAdd) {
        return leftOuterJoin(
                leftTable,
                rightTable,
                MatchPairFactory.getExpressions(columnsToMatch),
                MatchPairFactory.getExpressions(columnsToAdd));
    }

    /**
     * Returns a table that has one column for each of leftTable columns, and all the columns from rightTable whose
     * names don't overlap with the name of a column from leftTable. The returned table will have one row for each
     * matching set of keys between the left table and right table plus one row for any left table key set that doesn't
     * match the right table. Columns from the right table for which there was no match will have null values.
     * <p>
     * <p>
     *
     * @param leftTable input table
     * @param rightTable input table
     * @param columnsToMatch match criteria
     * @return a table that has one column for each of the left table columns, and one column corresponding to each
     *         column listed in columnsToAdd. If columnsToAdd.length==0 one column corresponding to each column of the
     *         right table columns whose names don't overlap with the name of a column from the left table is added. The
     *         returned table will have one row for each matching set of keys between the left table and right table
     *         plus one row for any left table key set that doesn't match the right table. Columns from the right table
     *         for which there was no match will have null values. Note that this method will cause tick expansion with
     *         ticking tables.
     */
    @ScriptApi
    public static Table leftOuterJoin(
            @NotNull final Table leftTable,
            @NotNull final Table rightTable,
            @NotNull final Collection<String> columnsToMatch) {
        return leftOuterJoin(leftTable, rightTable, columnsToMatch, Collections.emptyList());
    }

    /**
     * Returns a table that has one column for each of the left table columns, and one column corresponding to each of
     * the right table columns listed in the columns to add (or all the columns whose names don't overlap with the name
     * of a column from the source table if the columnsToAdd is length zero). The returned table will have one row for
     * each matching set of keys between the left table and right table plus one row for any left table key set that
     * doesn't match the right table. Columns from the right table for which there was no match will have null values.
     * Note that this method will cause tick expansion with ticking tables.
     * <p>
     *
     * @param leftTable input table
     * @param rightTable input table
     * @param columnsToMatch match criteria
     * @param columnsToAdd columns to add
     * @return a table that has one column for each of the left table columns, and one column corresponding to each
     *         column listed in columnsToAdd. If columnsToAdd.length==0 one column corresponding to each column of the
     *         right table columns whose names don't overlap with the name of a column from the left table is added. The
     *         returned table will have one row for each matching set of keys between the left table and right table
     *         plus one row for any left table key set that doesn't match the right table. Columns from the right table
     *         for which there was no match will have null values.
     */
    @ScriptApi
    public static Table leftOuterJoin(@NotNull final Table leftTable, @NotNull final Table rightTable,
            @NotNull final String columnsToMatch, @NotNull final String columnsToAdd) {
        return leftOuterJoin(leftTable, rightTable, TableOperationsDefaults.splitToCollection(columnsToMatch),
                TableOperationsDefaults.splitToCollection(columnsToAdd));
    }

    /**
     * Returns a table that has one column for each of leftTable columns, and all the columns from rightTable whose
     * names don't overlap with the name of a column from leftTable. The returned table will have one row for each
     * matching set of keys between the left table and right table plus one row for any left table key set that doesn't
     * match the right table. Columns from the right table for which there was no match will have null values.
     * <p>
     *
     * @param leftTable input table
     * @param rightTable input table
     * @param columnsToMatch match criteria
     * @return a table that has one column for each of the left table columns, and one column corresponding to each
     *         column listed in columnsToAdd. If columnsToAdd.length==0 one column corresponding to each column of the
     *         right table columns whose names don't overlap with the name of a column from the left table is added. The
     *         returned table will have one row for each matching set of keys between the left table and right table
     *         plus one row for any left table key set that doesn't match the right table. Columns from the right table
     *         for which there was no match will have null values. Note that this method will cause tick expansion with
     *         ticking tables.
     */
    @ScriptApi
    public static Table leftOuterJoin(
            @NotNull final Table leftTable,
            @NotNull final Table rightTable,
            @NotNull final String columnsToMatch) {
        return leftOuterJoin(leftTable, rightTable, TableOperationsDefaults.splitToCollection(columnsToMatch));
    }

    /**
     * Returns a table that has one column for each of table1 columns, and one column corresponding to each of table2
     * columns listed in the columns to add (or all the columns whose names don't overlap with the name of a column from
     * table1 if the columnsToAdd is length zero). The returned table will have one row for each matching set of keys
     * between the first and second tables, plus one row for any first table key set that doesn't match the second table
     * and one row for each key set from the second table that doesn't match the first table. Columns from the either
     * table for which there was no match in the other table will have null values. Note that this method will cause
     * tick expansion with ticking tables.
     * <p>
     *
     * @param table1 input table
     * @param table2 input table
     * @param columnsToMatch match criteria
     * @param columnsToAdd columns to add
     * @return a table that has one column for each of table1's columns, and one column corresponding to each of
     *         table2's columns listed in the columns to add (or all the columns whose names don't overlap with the name
     *         of a column from table1 if the columnsToAdd is length zero). The returned table will have one row for
     *         each matching set of keys between the first and second tables, plus one row for any first table key set
     *         that doesn't match the second table and one row for each key set from the second table that doesn't match
     *         the first table. Columns from the either table for which there was no match in the other table will have
     *         null values.
     */
    @ScriptApi
    public static Table fullOuterJoin(
            @NotNull final Table table1,
            @NotNull final Table table2,
            @NotNull final Collection<String> columnsToMatch,
            @NotNull final Collection<String> columnsToAdd) {
        return fullOuterJoin(table1, table2, MatchPairFactory.getExpressions(columnsToMatch),
                MatchPairFactory.getExpressions(columnsToAdd));
    }

    /**
     * Returns a table that has one column for each of table1 columns, and all the columns from table2 whose names don't
     * overlap with the name of a column from table1. The returned table will have one row for each matching set of keys
     * between the first and second tables, plus one row for any first table key set that doesn't match the second table
     * and one row for each key set from the second table that doesn't match the first table. Columns from the either
     * table for which there was no match in the other table will have null values. Note that this method will cause
     * tick expansion with ticking tables.
     * <p>
     * <p>
     *
     * @param table1 input table
     * @param table2 input table
     * @param columnsToMatch match criteria
     * @return a table that has one column for each of table1's columns, and one column corresponding to each of
     *         table2's columns listed in the columns to add (or all the columns whose names don't overlap with the name
     *         of a column from table1 if the columnsToAdd is length zero). The returned table will have one row for
     *         each matching set of keys between the first and second tables, plus one row for any first table key set
     *         that doesn't match the second table and one row for each key set from the second table that doesn't match
     *         the first table. Columns from the either table for which there was no match in the other table will have
     *         null values.
     */
    @ScriptApi
    public static Table fullOuterJoin(
            @NotNull final Table table1,
            @NotNull final Table table2,
            @NotNull final Collection<String> columnsToMatch) {
        return fullOuterJoin(table1, table2, columnsToMatch, Collections.emptyList());
    }

    /**
     * Returns a table that has one column for each of table1 columns, and one column corresponding to each of table2
     * columns listed in the columns to add (or all the columns whose names don't overlap with the name of a column from
     * table1 if the columnsToAdd is length zero). The returned table will have one row for each matching set of keys
     * between the first and second tables, plus one row for any first table key set that doesn't match the second table
     * and one row for each key set from the second table that doesn't match the first table. Columns from the either
     * table for which there was no match in the other table will have null values. Note that this method will cause
     * tick expansion with ticking tables.
     * <p>
     *
     * @param table1 input table
     * @param table2 input table
     * @param columnsToMatch match criteria
     * @param columnsToAdd columns to add
     * @return a table that has one column for each of table1's columns, and one column corresponding to each of
     *         table2's columns listed in the columns to add (or all the columns whose names don't overlap with the name
     *         of a column from table1 if the columnsToAdd is length zero). The returned table will have one row for
     *         each matching set of keys between the first and second tables, plus one row for any first table key set
     *         that doesn't match the second table and one row for each key set from the second table that doesn't match
     *         the first table. Columns from the either table for which there was no match in the other table will have
     *         null values.
     */
    @ScriptApi
    public static Table fullOuterJoin(
            @NotNull final Table table1,
            @NotNull final Table table2,
            @NotNull final String columnsToMatch,
            @NotNull final String columnsToAdd) {
        return fullOuterJoin(table1, table2, TableOperationsDefaults.splitToCollection(columnsToMatch),
                TableOperationsDefaults.splitToCollection(columnsToAdd));
    }

    /**
     * Returns a table that has one column for each of table1 columns, and all the columns from table2 whose names don't
     * overlap with the name of a column from table1. The returned table will have one row for each matching set of keys
     * between the first and second tables, plus one row for any first table key set that doesn't match the second table
     * and one row for each key set from the second table that doesn't match the first table. Columns from the either
     * table for which there was no match in the other table will have null values. Note that this method will cause
     * tick expansion with ticking tables.
     * <p>
     *
     * @param table1 input table
     * @param table2 input table
     * @param columnsToMatch match criteria
     * @return a table that has one column for each of table1's columns, and one column corresponding to each of
     *         table2's columns listed in the columns to add (or all the columns whose names don't overlap with the name
     *         of a column from table1 if the columnsToAdd is length zero). The returned table will have one row for
     *         each matching set of keys between the first and second tables, plus one row for any first table key set
     *         that doesn't match the second table and one row for each key set from the second table that doesn't match
     *         the first table. Columns from the either table for which there was no match in the other table will have
     *         null values.
     */
    @ScriptApi
    public static Table fullOuterJoin(
            @NotNull final Table table1,
            @NotNull final Table table2,
            @NotNull final String columnsToMatch) {
        return fullOuterJoin(table1, table2, TableOperationsDefaults.splitToCollection(columnsToMatch));
    }
}
