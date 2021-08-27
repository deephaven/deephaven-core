package io.deephaven.db.v2.utils;

import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.sources.ArrayBackedColumnSource;
import io.deephaven.util.type.TypeUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Class to aid in building Tables from a TableDefinition.
 */
@Deprecated
public class TableBuilder {
    private final TableDefinition def;
    private final List<Object[]> rows;

    /**
     * Creates a TableBuilder object based on a table Definition.
     *
     * @param def the definition of the table that you want to build
     */
    public TableBuilder(TableDefinition def) {
        this(def, 1000);
    }

    public TableBuilder(TableDefinition def, int initialSize) {
        this.def = def;
        rows = new ArrayList<>(initialSize);
    }

    /**
     * returns the number of rows the table has
     *
     * @return the size of the row List
     */
    public int rowCount() {
        return rows.size();
    }

    /**
     * Adds a row to the table. Items will be inserted into the row the order they are put into this method
     *
     * @param items The items that will appear in the row. Must be the same amount of items as columns
     */
    public void addRow(Object... items) {
        checkRow(items);
        rows.add(items);
    }

    /**
     * Checks if a the right number of items were added and that they were the right type.
     *
     * @param items the item array to be checked
     */
    private void checkRow(Object[] items) {
        List<Class<?>> colTypes = def.getColumnTypes();
        if (items.length != colTypes.size()) {
            throw new IllegalArgumentException(
                    "Incorrect column count: expected " + colTypes.size() + " got " + items.length);
        }

        for (int i = 0; i < colTypes.size(); i++) {
            if (items[i] != null && !TypeUtils.getUnboxedTypeIfBoxed(colTypes.get(i))
                    .isAssignableFrom(TypeUtils.getUnboxedTypeIfBoxed(items[i].getClass()))) {
                throw new IllegalArgumentException("Incorrect type for column " + def.getColumnNames().get(i)
                        + ": expected " + colTypes.get(i).getName()
                        + " got " + items[i].getClass().getName());
            }
        }

    }

    /**
     * Builds the table from the TableDefinition and the rows added
     *
     * @return the table
     */
    public Table build() {
        Map<String, ArrayBackedColumnSource<Object>> map = new LinkedHashMap<>();
        for (ColumnDefinition<?> columnDefinition : def.getColumns()) {
            ArrayBackedColumnSource<?> cs = ArrayBackedColumnSource.getMemoryColumnSource(
                    rows.size(), columnDefinition.getDataType());
            // noinspection unchecked
            map.put(columnDefinition.getName(), (ArrayBackedColumnSource<Object>) cs);
        }

        // Re-write column oriented
        int col = 0;
        for (ArrayBackedColumnSource<Object> source : map.values()) {
            for (int row = 0; row < rowCount(); row++) {
                source.set(row, rows.get(row)[col]);
            }
            col++;
        }
        return new QueryTable(def, Index.FACTORY.getFlatIndex(rows.size()), map);
    }

    /**
     * Deletes all rows from the TableBuilder.
     */
    public void clear() {
        rows.clear();
    }
}
