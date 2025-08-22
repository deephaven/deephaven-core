//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.util.NameValidator;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.ListenerRecorder;
import io.deephaven.engine.table.impl.MergedListener;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Convert column values into column names for aggregated columns. This works similarly to a pivot table, except that it
 * has no depth and is instead flattened into a single Deephaven table. This table is then suitable for downstream
 * operations like any other table.
 *
 * <p>
 * The {@code keyedTranspose} operation takes a source table with a set of aggregations and produces a new table where
 * the columns specified in {@code rowByColumns} are the keys used for the aggregation, and the values for the columns
 * specified in {@code columnByColumns} are used for the column names. An optional set of {@code initialGroups} can be
 * provided to ensure that the output table contains the full set of aggregated columns, even if no data is present yet
 * in the source table.
 * </p>
 *
 * <p>
 * For example, given the following source table...
 *
 * <pre>
 *       Date|     Level
 * ----------+----------
 * 2025-08-05|INFO
 * 2025-08-05|INFO
 * 2025-08-06|WARN
 * 2025-08-07|ERROR
 * </pre>
 *
 * ... and the usage ...
 *
 * <pre>
 * Table t = keyedTranspose(source, List.of(AggCount("Count")), new String[] {"Date"}, new String[] {"Level"});
 * </pre>
 *
 * The expected output for table "t" is ...
 *
 * <pre>
 *       Date|                INFO|                WARN|               ERROR
 * ----------+--------------------+--------------------+--------------------
 * 2025-08-05|                   2|(null)              |(null)
 * 2025-08-06|(null)              |                   1|(null)
 * 2025-08-07|(null)              |(null)              |                   1
 * </pre>
 * </p>
 *
 * <p>
 * In the example, you can see that the column names (e.g. INFO, WARN, ERROR) are taken from the values occurring for
 * {@code Level}. But what if there are multiple {@code aggregations} or multiple {@code columnByColumns} specified? The
 * resulting column names may yield duplicates.
 *
 * To avoid conflicts, the column naming works according to the following contract:
 * <ul>
 * <li>If {@code aggregations} = 1 and {@code columnByColumns} = 1: Column names are the value of the
 * {@code columnByColumns} column. (ex. INFO, WARN)</li>
 * <li>If {@code aggregations} > 1: Column names are prefixed with the aggregation column name. (ex. Count_INFO,
 * MySum_INFO)</li>
 * <li>If {@code columnByColumns} > 1: Values for the original columns are separated by an underscore (ex. INFO_OTHER1,
 * WARN_OTHER2)</li>
 * <li>If Illegal Characters: Purge characters that are invalid for Deephaven column names. (ex. "1-2.3/4" becomes
 * "1234")</li>
 * <li>If Starts with Number: Add the prefix "column_" to the column name. (ex. column_123)</li>
 * <li>If Duplicate Column Name: Add a suffix to differentiate the columns. (ex. INFO, INFO2)</li>
 * </ul>
 *
 * Given the above contract, and to give you more control over the result, it may be necessary to sanitize data values
 * that may be used as column names before using {code keyedTranspose}. Otherwise, "12.34" could be translated to
 * "column_1234" instead of a more meaningful column name.
 * </p>
 */
public class KeyedTranspose {
    /**
     * The behavior when a new column is detected.
     */
    public enum NewColumnBehavior {
        /**
         * The result table reports an error when a new column is detected. This is the default behavior, which ensures
         * consistency between the result and a newly created keyedTranspose.
         */
        FAIL,
        /**
         * The result table ignores the new column. If a new column would have been created, then the result table
         * becomes inconsistent with a newly created keyedTranspose.
         */
        IGNORE,
    }

    /**
     * Transpose the source table using the specified aggregations, row and column keys.
     *
     * <p>
     * If a new column is detected, then the result table produces an error.
     * </p>
     *
     * @param source The source table to transpose.
     * @param aggregations The aggregations to apply to the source table.
     * @param rowByColumns The columns to use as row keys in the transposed table.
     * @param columnByColumns The columns whose values become the new aggregated columns.
     * @return A new transposed table with the specified aggregations applied.
     */
    public static Table keyedTranspose(final Table source, Collection<? extends Aggregation> aggregations,
            final Collection<? extends ColumnName> rowByColumns,
            final Collection<? extends ColumnName> columnByColumns) {
        return keyedTranspose(source, aggregations, rowByColumns, columnByColumns, null);
    }

    /**
     * Transpose the source table using the specified aggregations, row and column keys, and an initial set of groups.
     *
     * <p>
     * If a new column is detected, then the result table produces an error.
     * </p>
     *
     * @param source The source table to transpose.
     * @param aggregations The aggregations to apply to the source table.
     * @param rowByColumns The columns to use as row keys in the transposed table.
     * @param columnByColumns The columns whose values become the new aggregated columns.
     * @param initialGroups An optional initial set of groups to ensure all columns are present in the output.
     */
    public static Table keyedTranspose(final Table source, final Collection<? extends Aggregation> aggregations,
            final Collection<? extends ColumnName> rowByColumns, final Collection<? extends ColumnName> columnByColumns,
            final Table initialGroups) {
        return keyedTranspose(source, aggregations, rowByColumns, columnByColumns, initialGroups,
                NewColumnBehavior.FAIL);
    }

    /**
     * Transpose the source table using the specified aggregations, row and column keys, and an initial set of groups.
     *
     * @param source The source table to transpose.
     * @param aggregations The aggregations to apply to the source table.
     * @param rowByColumns The columns to use as row keys in the transposed table.
     * @param columnByColumns The columns whose values become the new aggregated columns.
     * @param initialGroups An optional initial set of groups to ensure all columns are present in the output.
     * @param newColumnBehavior The behavior when a new column would be added
     */
    public static Table keyedTranspose(final Table source, final Collection<? extends Aggregation> aggregations,
            final Collection<? extends ColumnName> rowByColumns, final Collection<? extends ColumnName> columnByColumns,
            final Table initialGroups, final NewColumnBehavior newColumnBehavior) {
        final QueryTable querySource = (QueryTable) source.coalesce();
        if (querySource.isRefreshing()) {
            querySource.getUpdateGraph().checkInitiateSerialTableOperation();
        }

        if (aggregations.isEmpty()) {
            throw new IllegalArgumentException("No aggregations defined");
        }

        if (rowByColumns.isEmpty()) {
            throw new IllegalArgumentException("No rowByColumns defined");
        }
        if (columnByColumns.isEmpty()) {
            throw new IllegalArgumentException("No columnByColumns defined");
        }

        final Set<ColumnName> allByColumns = getAllByColumns(rowByColumns, columnByColumns);
        final Set<String> allByColumnNames = allByColumns.stream().map(ColumnName::name)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        final String[] rowByColumnNames = rowByColumns.stream().map(ColumnName::name).toArray(String[]::new);
        final String[] columnByColumnNames = columnByColumns.stream().map(ColumnName::name).toArray(String[]::new);

        Table aggregatedComplete;
        if (initialGroups == null || initialGroups.isEmpty()) {
            aggregatedComplete = source.aggBy(aggregations, allByColumns);
        } else {
            aggregatedComplete = source.aggBy(aggregations, true, initialGroups, allByColumns);
        }
        final PartitionedTable partitionedTable = aggregatedComplete.partitionBy(columnByColumnNames);
        final Table tableOfTables = partitionedTable.table();
        if (tableOfTables.isEmpty()) {
            throw new IllegalArgumentException("The source table has no values for column by columns");
        }

        final List<ColumnSource<Object>> nameSources = partitionedTable.keyColumnNames().stream()
                .map(tableOfTables::getColumnSource).collect(Collectors.toList());
        final ColumnSource<?> tableSource = tableOfTables.getColumnSource(partitionedTable.constituentColumnName());
        final List<JoinInfo> joinInfos = new ArrayList<>();
        tableOfTables.getRowSet().forAllRowKeys(rowKey -> {
            final Table constituentTable = (Table) tableSource.get(rowKey);
            final String joinColumn = nameSources.stream().map(s -> String.valueOf(s.get(rowKey)))
                    .collect(Collectors.joining("_"));
            final JoinInfo joinInfo =
                    getAddJoinColumns(constituentTable, allByColumnNames, joinColumn, aggregations.size() > 1);
            joinInfos.add(joinInfo);
        });
        final MultiJoinInput[] mji = legalizeJoinColumnNames(joinInfos).stream()
                .map(j -> MultiJoinInput.of(j.constituentTable, rowByColumnNames, j.getColumnMappings()))
                .toArray(MultiJoinInput[]::new);
        final Table multiJoinResult = MultiJoinFactory.of(mji).table();
        if (newColumnBehavior == NewColumnBehavior.IGNORE || !source.isRefreshing()) {
            return multiJoinResult;
        }

        if (newColumnBehavior != NewColumnBehavior.FAIL) {
            throw new IllegalStateException("Unknown NewColumnBehavior " + newColumnBehavior);
        }

        final QueryTable copy = new QueryTable(multiJoinResult.getDefinition(), multiJoinResult.getRowSet(),
                multiJoinResult.getColumnSourceMap());

        final List<String> tableOrKeyColumnNames = new ArrayList<>(partitionedTable.keyColumnNames());
        tableOrKeyColumnNames.add(partitionedTable.constituentColumnName());
        final ModifiedColumnSet tableOrKeys =
                ((QueryTable) tableOfTables).newModifiedColumnSet(tableOrKeyColumnNames.toArray(String[]::new));
        final ModifiedColumnSet.Transformer identityTransformer =
                ((QueryTable) multiJoinResult).newModifiedColumnSetIdentityTransformer(copy);

        final ListenerRecorder resultRecorder = new ListenerRecorder("MultiJoin result", multiJoinResult, copy);
        final ListenerRecorder newConstituentRecorder = new ListenerRecorder("partition result", tableOfTables, copy);
        multiJoinResult.addUpdateListener(resultRecorder);
        tableOfTables.addUpdateListener(newConstituentRecorder);

        final MergedListener mergedListener = new MergedListener(List.of(resultRecorder, newConstituentRecorder),
                List.of(), "new group failure listener", copy) {
            @Override
            protected void process() {
                if (newConstituentRecorder.recordedVariablesAreValid()) {
                    if (newConstituentRecorder.getAdded().isNonempty()) {
                        // no good, need to throw an error
                        throw new IllegalStateException(
                                "New constituent table detected in keyedTranspose; consider setting newColumnBehavior to Ignore.");
                    }
                    if (newConstituentRecorder.getModified().isNonempty()
                            && newConstituentRecorder.getModifiedColumnSet().containsAny(tableOrKeys)) {
                        throw new IllegalStateException(
                                "Modified constituent table detected in keyedTranspose; consider setting newColumnBehavior to Ignore.");
                    }
                    // removed or shifts don't matter
                }

                if (resultRecorder.recordedVariablesAreValid()) {
                    final TableUpdate upstream = resultRecorder.getUpdate();
                    final ModifiedColumnSet mcs = copy.getModifiedColumnSetForUpdates();
                    identityTransformer.clearAndTransform(upstream.modifiedColumnSet(), mcs);
                    final TableUpdateImpl downstream = new TableUpdateImpl(upstream.added().copy(),
                            upstream.removed().copy(),
                            upstream.modified().copy(),
                            upstream.shifted(),
                            mcs);
                    copy.notifyListeners(downstream);
                }
            }
        };

        resultRecorder.setMergedListener(mergedListener);
        newConstituentRecorder.setMergedListener(mergedListener);

        copy.addParentReference(mergedListener);

        return copy;
    }

    private static Set<ColumnName> getAllByColumns(final Collection<? extends ColumnName> rowByColumns,
            final Collection<? extends ColumnName> columnByColumns) {
        Set<ColumnName> columns = new LinkedHashSet<>(rowByColumns);
        columns.addAll(columnByColumns);
        return columns;
    }

    private static List<JoinInfo> legalizeJoinColumnNames(final List<JoinInfo> joinInfos) {
        String[] allLeftColumns = joinInfos.stream().flatMap(j -> j.leftColumns.stream()).toArray(String[]::new);
        allLeftColumns = NameValidator.legalizeColumnNames(allLeftColumns, true);
        final LinkedList<String> legalCols = new LinkedList<String>(Arrays.asList(allLeftColumns));
        for (JoinInfo info : joinInfos) {
            info.consumeAndUpdate(legalCols);
        }
        return joinInfos;
    }

    private static JoinInfo getAddJoinColumns(final Table table, final Set<String> allByColumnNames,
            final String joinColumn,
            final boolean usePrefix) {
        final JoinInfo info = new JoinInfo(table);
        for (String c : table.getDefinition().getColumnNames()) {
            if (allByColumnNames.contains(c))
                continue;
            info.addJoin((usePrefix ? (c + "_") : "") + joinColumn, c);
        }
        return info;
    }

    static class JoinInfo {
        final List<String> leftColumns;
        final List<String> rightColumns;
        final Table constituentTable;

        JoinInfo(Table constituentTable) {
            this.leftColumns = new ArrayList<>();
            this.rightColumns = new ArrayList<>();
            this.constituentTable = constituentTable;
        }

        void addJoin(String left, String right) {
            leftColumns.add(left);
            rightColumns.add(right);
        }

        String[] getColumnMappings() {
            final String[] mappings = new String[leftColumns.size()];
            for (int i = 0; i < leftColumns.size(); i++) {
                mappings[i] = leftColumns.get(i) + "=" + rightColumns.get(i);
            }
            return mappings;
        }

        void consumeAndUpdate(LinkedList<String> legalCols) {
            leftColumns.replaceAll(ignored -> legalCols.removeFirst());
        }
    }

}
