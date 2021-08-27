/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.v2.sources.ArrayBackedColumnSource;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.qst.column.Column;
import io.deephaven.qst.table.NewTable;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

public class InMemoryTable extends QueryTable {

    /**
     * Defers to {@link ArrayBackedColumnSource#from(io.deephaven.qst.array.Array)} to construct the appropriate
     * {@link ColumnSource column sources} (this involves copying the data).
     *
     * @param table the new table qst
     * @return the in memory table
     */
    public static InMemoryTable from(NewTable table) {
        final Map<String, ColumnSource<?>> columns = new LinkedHashMap<>(table.numColumns());
        for (Column<?> column : table) {
            final ColumnSource<?> source = ArrayBackedColumnSource.from(column.array());
            columns.put(column.name(), source);
        }
        return new InMemoryTable(
                TableDefinition.from(table.header()),
                Index.FACTORY.getFlatIndex(table.size()),
                columns);
    }

    public InMemoryTable(String[] columnNames, Object[] arrayValues) {
        super(Index.FACTORY.getFlatIndex(Array.getLength(arrayValues[0])), createColumnsMap(columnNames, arrayValues));
    }

    public InMemoryTable(TableDefinition definition, final int size) {
        super(Index.FACTORY.getFlatIndex(size),
                createColumnsMap(
                        definition.getColumnNames().toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY),
                        Arrays.stream(definition.getColumns()).map(
                                x -> Array.newInstance(x.getDataType(), size)).toArray(Object[]::new)));
    }

    private InMemoryTable(TableDefinition definition, Index index, Map<String, ? extends ColumnSource<?>> columns) {
        super(definition, index, columns);
    }

    private static Map<String, ColumnSource<?>> createColumnsMap(String[] columnNames, Object[] arrayValues) {
        Map<String, ColumnSource<?>> map = new LinkedHashMap<>();
        for (int i = 0; i < columnNames.length; i++) {
            map.put(columnNames[i], ArrayBackedColumnSource.getMemoryColumnSourceUntyped((arrayValues[i])));
        }
        return map;
    }
}
