/* Copyright (c) 2022-2025 Deephaven Data Labs and Patent Pending */
package io.deephaven.engine.table.impl.util;

import io.deephaven.api.agg.Aggregation;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.QueryTable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * KeyedTranspose: see ColumnsToRows for an example JavaDoc
 */
public class KeyedTranspose {

    public static Table keyedTranspose(Table source, Collection<? extends Aggregation> aggregations, String[] rowByColumns,
                                      String[] columnByColumns) {
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
        Table result = source.selectDistinct(rowByColumns);
        result = joinTransposedColumns(result, source, aggregations, rowByColumns, null, "Total");
        for(String byColumn: columnByColumns) {
            result = transposeColumnSet(result, source, aggregations, rowByColumns, byColumn);
        }
        return result;
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
