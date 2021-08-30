package io.deephaven.integrations.learn;

import io.deephaven.db.tables.utils.NameValidator;
import io.deephaven.integrations.python.PythonFunctionCaller;
import org.jpy.PyObject;

import io.deephaven.base.verify.Require;

import java.util.function.Function;

/**
 * Output specifies how to scatter data from a Python object into a table column.
 */
public class Output {

    private final String colName;
    private final Function<Object[], Object> scatterFunc;
    private final String type;

    /**
     * Creates a new Output.
     *
     * @param colName name of new column to store results.
     * @param scatterFunc function to scatter the results of a Python object into the table column.
     * @param type desired datatype of the new column.
     */
    public Output(String colName, PyObject scatterFunc, String type) {
        this(colName, new PythonFunctionCaller(Require.neqNull(scatterFunc, "scatterFunc")), type);
    }

    /**
     * Creates a new Output.
     *
     * @param colName name of new column to store results.
     * @param scatterFunc function to scatter the results of a Python object into the table column.
     * @param type desired datatype of the new column.
     */
    public Output(String colName, Function<Object[], Object> scatterFunc, String type) {

        Require.neqNull(colName, "colName");
        Require.neqNull(scatterFunc, "scatterFunc");

        NameValidator.validateColumnName(colName);

        this.colName = colName;
        this.scatterFunc = scatterFunc;
        this.type = type;
    }

    /**
     * Gets the output column name.
     *
     * @return the output column name.
     */
    public String getColName() {
        return colName;
    }

    /**
     * Gets the scatter function.
     *
     * @return the scatter function.
     */
    public Function<Object[], Object> getScatterFunc() {
        return scatterFunc;
    }

    /**
     * Gets the type of the output column.
     *
     * @return the output column datatype.
     */
    public String getType() {
        return type;
    }

    @Override
    public String toString() {
        return "Output{" +
                "colName='" + colName + '\'' +
                ", scatterFunc=" + scatterFunc +
                ", type='" + type + '\'' +
                '}';
    }
}
