package io.deephaven.integrations.learn;

import io.deephaven.db.tables.Table;
import io.deephaven.db.v2.sources.ColumnSource;
import org.jpy.PyObject;

/**
 * This class provides an interface for converting Deephaven tables to objects that Python deep learning libraries
 * are familiar with. Input objects are intended to be used as the input argument of an eval() function call,
 * and they provide methods for converting column names and tables directly into ColumnSources.
 */

public class Input {

    final Table table;
    final String[] colNames;
    final PyObject func;

    /**
     * Constructor for Input object. Initializes variables needed to create column sources from given inputs.
     *
     * @param colNames The array of column names from a Deephaven table to be used in modelling.
     * @param func The function that determines how data from a Deephaven table is collected.
     */
    public Input(Table table, String[] colNames, PyObject func) {

        this.table = table;
        this.colNames = colNames;
        this.func = func;
    }

    /**
     * Creates a list of Deephaven ColumnSources to be used by gather functions.
     */
    public ColumnSource<?>[] createColumnSource() {

        ColumnSource<?>[] colSet = new ColumnSource[colNames.length];
        for (int i = 0 ; i < colNames.length ; i++) {
            colSet[i] = this.table.getColumnSource(colNames[i]);
        }

        return colSet;
    }
}