/* Copyright (c) 2022-2025 Deephaven Data Labs and Patent Pending */
package io.deephaven.engine.table.impl.util;

import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.util.NameValidator;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.QueryTable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Convert column values into column names for aggregated columns. This works similarly to a pivot table, except that
 * it has no depth and is instead flattened into a single Deephaven table. This table is then suitable for
 * downstream operations like any other table.
 *
 * <p>
 * The {@code keyedTranspose} operation takes a source table with a set of aggregations and produces a new table where the
 * columns specified in {@code rowByColumns} are the keys used for the aggregation, and the values for the columns
 * specified in {@code columnByColumns} are used for the column names. An optional set of {@code initialGroups} can be
 * provided to ensure that the output table contains the full set of aggregated columns, even if no data is present yet
 * in the source table.
 * </p>
 *
 <p>
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
 * Table t = keyedTranspose(source, List.of(AggCount("Count")), new String[]{"Date"}, new String[]{"Level"});
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
 */
public class KeyedTranspose {

    /**
     * Transpose the source table using the specified aggregations, row and column keys.
     *
     * @param source The source table to transpose.
     * @param aggregations The aggregations to apply to the source table.
     * @param rowByColumns The columns to use as row keys in the transposed table.
     * @param columnByColumns The columns whose values become the new aggregated columns.
     * @return A new transposed table with the specified aggregations applied.
     */
    public static Table keyedTranspose(Table source, Collection<? extends Aggregation> aggregations, String[] rowByColumns,
                                       String[] columnByColumns) {
        return keyedTranspose(source, aggregations, rowByColumns, columnByColumns, null);
    }

    /**
     * Transpose the source table using the specified aggregations, row and column keys, and an initial set of groups.
     *
     * @param source The source table to transpose.
     * @param aggregations The aggregations to apply to the source table.
     * @param rowByColumns The columns to use as row keys in the transposed table.
     * @param columnByColumns The columns whose values become the new aggregated columns.
     * @param initialGroups An optional initial set of groups to ensure all columns are present in the output.
     */
    public static Table keyedTranspose(Table source, Collection<? extends Aggregation> aggregations, String[] rowByColumns,
                                      String[] columnByColumns, Table initialGroups) {
        QueryTable querySource = (QueryTable) source.coalesce();
        if (rowByColumns.length == 0) {
            throw new IllegalArgumentException("No rowByColumns defined");
        }
        if (columnByColumns.length == 0) {
            throw new IllegalArgumentException("No columnByColumns defined");
        }
        if (aggregations.isEmpty()) {
            throw new IllegalArgumentException("No aggregations defined");
        }

        Set<ColumnName> allByColumns = getAllByColumns(rowByColumns, columnByColumns);
        Set<String> allByColumnNames = allByColumns.stream().map(ColumnName::name)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        Table aggregatedComplete;
        if(initialGroups == null || initialGroups.isEmpty()) {
            aggregatedComplete = source.aggBy(aggregations, allByColumns);
        } else {
            aggregatedComplete = source.aggBy(aggregations, true, initialGroups, allByColumns);
        }
        PartitionedTable partitionedTable = aggregatedComplete.partitionBy(columnByColumns);
        Table tableOfTables = partitionedTable.table();

        List<ColumnSource<Object>> nameSources = partitionedTable.keyColumnNames().stream()
                .map(tableOfTables::getColumnSource).collect(Collectors.toList());
        ColumnSource<?> tableSource = tableOfTables.getColumnSource(partitionedTable.constituentColumnName());
        List<JoinInfo> joinInfos = new ArrayList<>();
        tableOfTables.getRowSet().forEachRowKey(rowKey -> {
            Table constituentTable = (Table)tableSource.get(rowKey);
            String joinColumn = nameSources.stream().map(s-> String.valueOf(s.get(rowKey)))
                    .collect(Collectors.joining("_"));
            JoinInfo joinInfo = getAddJoinColumns(constituentTable, allByColumnNames, joinColumn, aggregations.size() > 1);
            joinInfos.add(joinInfo);
            return true;
        });
        MultiJoinInput[] mji = legalizeJoinColumnNames(joinInfos).stream()
                .map(j -> MultiJoinInput.of(j.constituentTable, rowByColumns, j.getColumnMappings()))
                .toArray(MultiJoinInput[]::new);
        return MultiJoinFactory.of(mji).table();
    }

    private static Set<ColumnName> getAllByColumns(String[] rowByColumns, String[] columnByColumns) {
        return Stream.concat(Arrays.stream(rowByColumns), Arrays.stream(columnByColumns))
                .map(ColumnName::of).collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private static List<JoinInfo> legalizeJoinColumnNames(List<JoinInfo> joinInfos) {
        String[] allLeftColumns = joinInfos.stream().flatMap(j -> j.leftColumns.stream()).toArray(String[]::new);
        allLeftColumns = NameValidator.legalizeColumnNames(allLeftColumns, true);
        LinkedList<String> legalCols = new LinkedList<String>(Arrays.asList(allLeftColumns));
        for(JoinInfo info: joinInfos) {
            info.consumeAndUpdate(legalCols);
        }
        return joinInfos;
    }

    private static JoinInfo getAddJoinColumns(Table table, Set<String> allByColumnNames, String joinColumn,
                                              boolean usePrefix) {
        JoinInfo info = new JoinInfo(table);
        for(String c: table.getDefinition().getColumnNames()) {
            if(allByColumnNames.contains(c)) continue;
            info.addJoin((usePrefix?(c+"_"):"") + joinColumn, c);
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
            String[] mappings = new String[leftColumns.size()];
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