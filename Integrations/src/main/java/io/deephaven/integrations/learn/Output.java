package io.deephaven.integrations.learn;

import org.jpy.PyObject;

/**
 * This class provides an interface for converting Python deep learning output to Deephaven table columns.
 * Output objects are intended to be used as the input argument of an eval() function call.
 */

public class Output {

    final String colNames;
    final PyObject func;
    final String type;

    /**
     * Constructor for Output object.
     *
     * @param colNames The array of column names from a Deephaven table.
     * @param func     The function that determines how data from a Python object is scattered back to a Deephaven table.
     * @param type     The datatype that the user wishes to return.
     */
    public Output(String colNames, PyObject func, String type) {

        this.colNames = colNames;
        this.func = func;
        this.type = type;
    }

    /**
     * Getter method for column names.
     *
     * @return String array of column names provided by the user
     */
    public String getColName() { return this.colNames; }

    /**
     * Getter method for Python function.
     *
     * @return Python function provided by the user.
     */
    public PyObject getFunc() { return this.func; }

    /**
     * Getter method for type.
     *
     * @return String containing type provided by the user.
     */
    public String getType() { return this.type; }
}