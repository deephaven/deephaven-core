/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.v2.sources.ArrayBackedColumnSource;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.utils.Index;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

public class InMemoryTable extends QueryTable {

    public InMemoryTable(String columnNames[], Object arrayValues[]) {
        super(Index.FACTORY.getFlatIndex(Array.getLength(arrayValues[0])), createColumnsMap(columnNames, arrayValues));
    }

    public InMemoryTable(TableDefinition definition, final int size) {
        super(Index.FACTORY.getFlatIndex( size ),
                createColumnsMap(
                        definition.getColumnNames().toArray(new String[definition.getColumnNames().size()]),
                        Arrays.stream(definition.getColumns()).map(
                                x -> Array.newInstance(x.getDataType(), size)).toArray(Object[]::new)));
    }

    private static Map<String, ColumnSource> createColumnsMap(String[] columnNames, Object[] arrayValues) {
        Map<String, ColumnSource> map = new LinkedHashMap<>();
        for (int i = 0; i < columnNames.length; i++) {
            map.put(columnNames[i], ArrayBackedColumnSource.getMemoryColumnSourceUntyped((arrayValues[i])));
        }
        return map;
    }
}
