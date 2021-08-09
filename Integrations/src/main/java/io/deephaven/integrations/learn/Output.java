package io.deephaven.integrations.learn;

import io.deephaven.db.tables.utils.NameValidator;
import io.deephaven.integrations.python.PythonFunctionCaller;
import org.jpy.PyObject;

import io.deephaven.base.verify.Require;

/**
 * Output specifies how to scatter data from a Python object into a table column.
 */
public class Output {

    private final String colName;
    private final PythonFunctionCaller scatterCaller;
    private final String type;

    /**
     * Creates a new Output.
     *
     * @param colName       name of new column to store results.
     * @param scatterFunc   function to scatter the results of a Python object into the table column.
     * @param type          desired datatype of the new column.
     */
    public Output(String colName, PyObject scatterFunc, String type) {

        Require.neqNull(colName, "colName");
        Require.neqNull(scatterFunc, "scatterFunc");
        if (type == null) {
            type = "Java.lang.Object";
        }

        NameValidator.validateColumnName(colName);

        this.colName = colName;
        this.scatterCaller = new PythonFunctionCaller(scatterFunc);
        this.type = type;
    }

    /**
     * Gets the output column name.
     *
     * @return the output column name.
     */
    public String getColName() { return colName; }

    /** Gets the scatter function caller.
     *
     * @return the scatter function caller.
     */
    public PythonFunctionCaller getScatterCaller() { return scatterCaller; }

    /**
     * Gets the type of the output column.
     *
     * @return the output column datatype.
     */
    public String getType() { return type; }
}