/* Copyright (c) 2022-2025 Deephaven Data Labs and Patent Pending */
package io.deephaven.engine.table.impl.util;

import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.util.TableTools;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * KeyedTranspose: see ColumnsToRows for an example JavaDoc
 */
public class KeyedTranspose {

    public static Table keyedTranspose(Table source, Collection<? extends Aggregation> aggregations, String[] rowByColumns,
                                       String[] columnByColumns) {
        return keyedTranspose(source, aggregations, rowByColumns, columnByColumns, null);
    }

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
        String[] allByColumnNames = allByColumns.stream().map(ColumnName::name).toArray(String[]::new);
        Table aggregatedComplete;
        if(initialGroups == null || initialGroups.isEmpty()) {
            aggregatedComplete = source.aggBy(aggregations, allByColumns);
        } else {
            aggregatedComplete = source.aggBy(aggregations, true, initialGroups, allByColumns);
        }
        PartitionedTable partitionedTable = aggregatedComplete.partitionBy(columnByColumns);
        Table tableOfTables = partitionedTable.table();
        TableTools.show(tableOfTables, 5);

        List<MultiJoinInput> mji = new ArrayList<>(tableOfTables.intSize());
        List<ColumnSource<Object>> nameSources = partitionedTable.keyColumnNames().stream().map(tableOfTables::getColumnSource)
                .collect(Collectors.toList());
        ColumnSource<?> tableSource = tableOfTables.getColumnSource(partitionedTable.constituentColumnName());
        tableOfTables.getRowSet().forEachRowKey(rowKey -> {
            Table constituentTable = (Table)tableSource.get(rowKey);
            TableTools.show(constituentTable, 5);

            if(constituentTable != null) {
                String joinColumn = nameSources.stream().map(s-> s.get(rowKey).toString()).collect(Collectors.joining("_"));
                String[] addJoinColumns = getAddJoinColumns(constituentTable, allByColumnNames, joinColumn, aggregations.size() > 1);
                System.out.println("addJoinColumns: " + Arrays.toString(addJoinColumns));
                mji.add(MultiJoinInput.of(constituentTable, rowByColumns, addJoinColumns));
            }
            return true;
        });
        return MultiJoinFactory.of(mji.toArray(MultiJoinInput[]::new)).table();
    }

    private static Set<ColumnName> getAllByColumns(String[] rowByColumns, String[] columnByColumns) {
        return Stream.concat(Arrays.stream(rowByColumns), Arrays.stream(columnByColumns))
                .map(ColumnName::of).collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private static String[] getAddJoinColumns(Table table, String[] allByColumns, String joinColumn, boolean usePrefix) {
        return table.getDefinition().getColumnNames().stream()
                .filter(c -> !Arrays.asList(allByColumns).contains(c)).map(c-> (usePrefix?(c+"_"):"") + joinColumn + '=' + c)
                .toArray(String[]::new);
    }

    private static String getJoinMappings(Table right, String[] rowByColumns, Object suffix) {
        return right.getDefinition().getColumnNames().stream().filter(c->!Arrays.asList(rowByColumns).contains(c))
                .map(c-> c + '_' + suffix + '=' + c).collect(Collectors.joining(","));
    }

    private static Table joinTransposedColumns(Table result, Table source, Collection<? extends Aggregation> aggregations,
                                               String[] rowByColumns, String byColumn, Object byValue) {
        String byValueStr = byValue instanceof Number?("" + byValue):("`" + byValue + "`");
        Table right;
        if(byColumn == null) right = source.aggBy(aggregations, rowByColumns);
        else right = source.where(byColumn + '=' + byValueStr).aggBy(aggregations, rowByColumns);
        String joinMappings = getJoinMappings(right, rowByColumns, byValue);
        return result.naturalJoin(right, String.join(",", rowByColumns), joinMappings);
    }

    private static Table transposeColumnSet(Table result, Table source, Collection<? extends Aggregation> aggregations,
                                            String[] rowByColumns, String byColumn) {
        Table byValues = source.selectDistinct(byColumn).sort();

        try(CloseableIterator<String> iterator = byValues.columnIterator(byColumn);) {
            while (iterator.hasNext()) {
                Object byValue = iterator.next();
                result = joinTransposedColumns(result, source, aggregations, rowByColumns, byColumn, byValue);
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException("Error transposing column: " + byColumn, e);
        }
    }

}
