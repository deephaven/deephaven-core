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

    final String[] colNames;
    final ColumnSource<?>[] colSet;
    final PyObject func;

    /**
     * Constructor for Input object. Initializes variables needed to create column sources from given inputs.
     *
     * @param colNames The array of column names from a Deephaven table to be used in modelling.
     * @param func The function that determines how data from a Deephaven table is collected.
     */
    public Input(String[] colNames, PyObject func) {
        this.colNames = colNames;
        this.colSet = new ColumnSource[colNames.length];
        this.func = func;
    }

    /**
     * Creates an array list of Deephaven ColumnSources to be used by gather functions.
     *
     * @param table Deephaven table containing the columns that colNames should pull from.
     */
    void createColumnSource(Table table) {
        for (int i = 0 ; i < colNames.length ; i++) {
            this.colSet[i] = table.getColumnSource(colNames[i]);
        }
    }

    public void printMe() {
        System.out.println(this.colNames);
        System.out.println(this.colSet);
        System.out.println(this.func);
    }

    public void printType() {
        System.out.println(this.colNames.getClass().getSimpleName());
        System.out.println(this.colSet.getClass().getSimpleName());
        System.out.println(this.func.getClass().getSimpleName());
    }
}
