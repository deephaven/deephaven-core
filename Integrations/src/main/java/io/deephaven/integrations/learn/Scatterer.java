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

    final int batchSize;
    final Output[] outputs;
    PyObject scattered;
    int currentOutputIndex;

    /**
     * Constructor for Scatterer. Initializes the scatterer and sets the count to -1,
     * @param batchSize
     * @param outputs
     */

    public Scatterer(int batchSize, Output ... outputs) {

        this.batchSize = batchSize;
        this.outputs = outputs;
    }

    public void clear() {
        //this.count = -1;
    }

    public Output[] getOutputs() {
        return this.outputs;
    }

    public PyObject scatter(PyObject result, PyObject scatterFunc, long offset, int i, int j) {
        PythonFunction<PyObject> scatterCaller = new PythonFunction<>(scatterFunc, PyObject.class);
        this.scattered = scatterCaller.passThroughScatter(result, offset);
        return this.scattered;
    }

    public String[] generateQueryStrings() {
        ArrayList<String> queryStrings = new ArrayList<String>();

        for (int i = 0 ; i < this.outputs.length ; i++) {
            for (int j = 0; j < this.outputs[i].getColNames().length; j++) {
                queryStrings.add(String.format("%s = scatterer.scatter(Future.get(), scatterer.getOutputs()[%d].getFunc(), Future.getOffset())", this.outputs[i].getColNames()[j], i));
            }
        }
        return queryStrings.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    }
}