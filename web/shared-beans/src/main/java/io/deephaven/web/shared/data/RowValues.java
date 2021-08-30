package io.deephaven.web.shared.data;

import java.io.Serializable;
import java.util.Arrays;

/**
 * A container for an array of columns to add to an input table.
 *
 */
public class RowValues implements Serializable {

    private ColumnValue[] columns;

    public RowValues() {}

    public RowValues(ColumnValue... values) {
        columns = values;
    }

    public ColumnValue[] getColumns() {
        return columns;
    }

    public void setColumns(ColumnValue[] columns) {
        this.columns = columns;
    }

    @Override
    public String toString() {
        return "RowValues{" +
            "columns=" + Arrays.toString(columns) + '}';
    }

    public boolean isEmpty() {
        return columns.length == 0;
    }
}
