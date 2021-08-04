package io.deephaven.integrations.learn;

import io.deephaven.integrations.python.PythonFunction;
import org.jpy.PyObject;

/**
 * The Scatterer is an interface for getting data out of Python objects and back into Deephaven tables via user-defined
 * functions. This will take in a Python object generated from a call to Future.get() as well as a Python function that
 * determines how data from that object should be arranged into the rows of a Deephaven column, and will return a Python
 * object that can then be cast to the desired type.
 */

public class Scatterer {

    final int batchSize;
    final Output outputs;
    int count;
    PyObject scattered;

    public Scatterer(int batchSize, Output outputs) {

        this.batchSize = batchSize;
        this.outputs = outputs;
        this.count = -1;
    }

    public void clear() {
        this.count = -1;
    }

    public PyObject scatter(PyObject result, long idx) {
        PythonFunction<PyObject> scatterCaller = new PythonFunction<>(this.outputs.func, PyObject.class);
        this.scattered = scatterCaller.passThroughScatter(result, idx);
        return this.scattered;
    }

    public int getBatchSize() {
        return this.batchSize;
    }

}
