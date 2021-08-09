package io.deephaven.integrations.learn;

import io.deephaven.db.tables.Table;
import io.deephaven.db.v2.sources.ColumnSource;
import org.jpy.PyObject;

/**
 * Input specifies how to gather data from a table into a Python object.
 */
public class Input {

    private final String[] colNames;
    private final PyObject gatherFunc;

    /**
     * Creates a new Input.
     *
     * @param colNames      array of column names to be used as inputs.
     * @param gatherFunc    function that gathers data into a Python object.
     */
    public Input(String[] colNames, PyObject gatherFunc) {

        this.colNames = colNames;
        this.gatherFunc = gatherFunc;

    }

    /** Creates an array of column sources specified by this table and given column names.
     *
     * @return column sources needed to generate the input.
     */
    ColumnSource<?>[] createColumnSource(Table table) {

        ColumnSource<?>[] colSet = new ColumnSource[colNames.length];
        for (int i = 0 ; i < colNames.length ; i++) {
            colSet[i] = table.getColumnSource(colNames[i]);
        }

        return colSet;
    }

    /**
     * Get the gather function.
     *
     * @return the gather function.
     */
    PyObject getFunc() { return gatherFunc; }
}
