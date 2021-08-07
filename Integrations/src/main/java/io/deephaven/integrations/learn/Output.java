package io.deephaven.integrations.learn;

import org.jpy.PyObject;

/**
 * Output specifies how to scatter data from a Python object into a table column.
 */
public class Output {

    private final String colName;
    private final PyObject scatterFunc;
    private final String type;

    /**
     * Creates a new Output.
     *
     * @param colName       name of new column to store results.
     * @param scatterFunc   function to scatter the results of a Python object into the table column.
     * @param type          desired datatype of the new column.
     */
    public Output(String colName, PyObject scatterFunc, String type) {

        this.colName = colName;
        this.scatterFunc = scatterFunc;
        this.type = type;
    }

    /**
     * Getter method for colName.
     *
     * @return column name for this output.
     */
    public String getColName() { return colName; }

    /** Getter method for scatterFunc.
     *
     * @return the scatter function.
     */
    public PyObject getFunc() { return scatterFunc; }

    /**
     * Getter method for type
     *
     * @return the output column datatype.
     */
    public String getType() { return type; }
}