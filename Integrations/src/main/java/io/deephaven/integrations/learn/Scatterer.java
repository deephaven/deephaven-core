package io.deephaven.integrations.learn;

import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.integrations.python.PythonFunction;
import org.jpy.PyObject;

import java.util.ArrayList;

/**
 * Applies scatter functions to the result of Future.get() and feeds the scattered data back into a table.
 */
public class Scatterer {

    private final Output[] outputs;

    /**
     * Creates a new Scatterer.
     *
     * @param outputs Array of Outputs that determine how data will be scattered back to the table.
     */
    public Scatterer(Output ... outputs) { this.outputs = outputs; }

    /**
     * Applies the scatter function to a subset of result.
     *
     * @param result Result of the call to Future.get() containing data to be scattered.
     * @param scatterFunc Function that determines how result will be parsed and fed into the table.
     * @param offset Offset from FutureOffset that gets correct row from result.
     * @return Subset of result that can be put back into the table.
     */
    public PyObject scatter(PyObject result, PyObject scatterFunc, long offset) {

        PythonFunction<PyObject> scatterCaller = new PythonFunction<>(scatterFunc, PyObject.class);
        return scatterCaller.pyObjectApply(result, offset);
    }

    /**
     * Generates a query string for each of these Outputs.
     *
     * @return List of query strings to be used in .update() call
     */
    public String[] generateQueryStrings() {

        ArrayList<String> queryStrings = new ArrayList<String>();
        for (int i = 0; i < this.outputs.length; i++) {
            queryStrings.add(String.format("%s = (scatterer.scatter(FutureOffset.getFuture().get(), scatterer.getOutputs()[%d].getFunc(), FutureOffset.getOffset()))", this.outputs[i].getColName(), i));
        }
        return queryStrings.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    }

    /** Returns these outputs. */
    public Output[] getOutputs() { return this.outputs; }
}