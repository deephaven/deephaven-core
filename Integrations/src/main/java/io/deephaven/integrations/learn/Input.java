package io.deephaven.integrations.learn;

import io.deephaven.db.tables.Table;
import io.deephaven.db.v2.sources.ColumnSource;
import org.jpy.PyObject;

/**
 * Provides an interface for getting data directly out of a table and into a function.
 */
public class Input {

    private final Table table;
    private final String[] colNames;
    private final PyObject func;

    /**
     * Creates a new Input.
     *
     * @param table Table that contains data to be used with this function.
     * @param colNames Array of column names to be extracted from this table.
     * @param func Function that determines how data from this table is collected.
     */
    public Input(Table table, String[] colNames, PyObject func) {

        this.table = table;
        this.colNames = colNames;
        this.func = func;
    }

    /** Creates an array of column sources specified by this table and given column names.
     *
     * @return Column sources with these column names from this table.
     */
    public ColumnSource<?>[] createColumnSource() {

        ColumnSource<?>[] colSet = new ColumnSource[this.colNames.length];
        for (int i = 0 ; i < this.colNames.length ; i++) {
            colSet[i] = this.table.getColumnSource(this.colNames[i]); }

        return colSet;
    }

    /** Returns this function. */
    public PyObject getFunc() { return this.func; }
}