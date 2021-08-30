package io.deephaven.integrations.learn;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.utils.NameValidator;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.integrations.python.PythonFunctionCaller;
import io.deephaven.util.annotations.ScriptApi;
import org.jpy.PyObject;

import io.deephaven.base.verify.Require;

import java.util.Arrays;
import java.util.function.Function;

/**
 * Input specifies how to gather data from a table into a Python object.
 */
public class Input {

    private final String[] colNames;
    private final Function<Object[], Object> gatherFunc;

    /**
     * Creates a new Input.
     *
     * @param colName column name to be used as input.
     * @param gatherFunc function that gathers data into a Python object.
     */
    public Input(String colName, PyObject gatherFunc) {
        this(new String[] {colName}, new PythonFunctionCaller(Require.neqNull(gatherFunc, "gatherFunc")));
    }

    /**
     * Creates a new Input.
     *
     * @param colNames array of column names to be used as inputs.
     * @param gatherFunc function that gathers data into a Python object.
     */
    public Input(String[] colNames, PyObject gatherFunc) {
        this(colNames, new PythonFunctionCaller(Require.neqNull(gatherFunc, "gatherFunc")));
    }

    /**
     * Creates a new Input.
     *
     * @param colName column name to be used as inputs.
     * @param gatherFunc function that gathers data into a Python object.
     */
    public Input(String colName, Function<Object[], Object> gatherFunc) {
        this(new String[] {colName}, gatherFunc);
    }

    /**
     * Creates a new Input.
     *
     * @param colNames array of column names to be used as inputs.
     * @param gatherFunc function that gathers data into a Python object.
     */
    public Input(String[] colNames, Function<Object[], Object> gatherFunc) {

        Require.neqNull(colNames, "colNames");
        Require.neqNull(gatherFunc, "gatherFunc");

        for (int i = 0; i < colNames.length; i++) {
            NameValidator.validateColumnName(colNames[i]);
        }

        this.colNames = colNames;
        this.gatherFunc = gatherFunc;
    }

    /**
     * Creates an array of column sources specified by this table and given column names.
     *
     * @return column sources needed to generate the input.
     */
    ColumnSource<?>[] createColumnSource(Table table) {

        ColumnSource<?>[] colSet = new ColumnSource[colNames.length];

        for (int i = 0; i < colNames.length; i++) {
            colSet[i] = table.getColumnSource(colNames[i]);
        }

        return colSet;
    }

    /**
     * Gets the gather function.
     *
     * @return the gather function.
     */
    Function<Object[], Object> getGatherFunc() {
        return gatherFunc;
    }

    /**
     * Gets the column names.
     *
     * @return the column names.
     */
    @ScriptApi
    public String[] getColNames() {
        return colNames;
    }

    @Override
    public String toString() {
        return "Input{" +
                "colNames=" + Arrays.toString(colNames) +
                ", gatherFunc=" + gatherFunc +
                '}';
    }
}
