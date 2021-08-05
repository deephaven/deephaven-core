package io.deephaven.integrations.learn;

import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.integrations.python.PythonFunction;
import org.jpy.PyObject;

import java.util.ArrayList;

/**
 * The Scatterer is an interface for getting data out of Python objects and back into Deephaven tables via user-defined
 * functions. This will take in a Python object generated from a call to Future.get() as well as a Python function that
 * determines how data from that object should be arranged into the rows of a Deephaven column, and will return a Python
 * object that can then be cast to the desired type.
 */

public class Scatterer {

    final Output[] outputs;
    PyObject scattered;

    /**
     * Constructor for Scatterer.
     *
     * @param outputs
     */
    public Scatterer(Output ... outputs) { this.outputs = outputs; }

    /**
     * Getter method for Outputs.
     *
     * @return Array of Deephaven Output objects.
     */
    public Output[] getOutputs() { return this.outputs; }

    /**
     *
     * @param result
     * @param scatterFunc
     * @param offset
     * @return
     */
    public PyObject scatter(PyObject result, PyObject scatterFunc, long offset) {

        PythonFunction<PyObject> scatterCaller = new PythonFunction<>(scatterFunc, PyObject.class);
        this.scattered = scatterCaller.pyObjectApply(result, offset);
        return this.scattered;
    }

    /**
     * Uses user input via Output objects to create a list of Query strings that are used to produce new columns
     * with the scattered result, specified by the user.
     *
     * @return List of query strings to be used in .update() call
     */
    public String[] generateQueryStrings() {

        ArrayList<String> queryStrings = new ArrayList<String>();
        for (int i = 0 ; i < this.outputs.length ; i++) {
            queryStrings.add(String.format("%s = scatterer.scatter(FutureOffset.getFuture().get(), scatterer.getOutputs()[%d].getFunc(), FutureOffset.getOffset())", this.outputs[i].getColName(), i));
        }

        return queryStrings.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    }
}