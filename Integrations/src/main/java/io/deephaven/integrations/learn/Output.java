package io.deephaven.integrations.learn;

import org.jpy.PyObject;

/**
 * Provides an interface for getting data out of a Python object and into a table.
 */
public class Output {

    private final String colName;
    private final PyObject func;
    private final String type;

    /**
     * Creates a new Output.
     *
     * @param colName  Name of new column to store results.
     * @param func     Function that determines how results of Future.get() are scattered back to a table.
     * @param type     Desired datatype of new column.
     */
    public Output(String colName, PyObject func, String type) {

        this.colName = colName;
        this.func = func;
        this.type = type;
    }

    /** Returns this column name. */
    public String getColName() { return this.colName; }

    /** Returns this function. */
    public PyObject getFunc() { return this.func; }

    /** Returns this type. */
    public String getType() { return this.type; }
}