package io.deephaven.integrations.learn;

import io.deephaven.integrations.python.PythonFunction;
import org.jpy.PyObject;

/**
 * This class provides the machinery for doing computations on index sets with user-provided Python functions. Its
 * key feature is that it will only perform these computations once, and other classes can access those results
 * as many times as needed without recomputing.
 *
 * This is the second layer of this design, as a Future is only ever instantiated by a Computer. This is also where all
 * computations take place, so is the focal point for future optimization work.
 */

public class Future {

    final PyObject func;
    final int batchSize;
    final Input[] inputs;
    final IndexSet indexSet;
    boolean called;
    PyObject result;

    /**
     * Constructor for Future. Creates an IndexSet of maximum size batchSize and initializes necessary fields.
     *
     * @param func Python function to use for computation with gathered data.
     * @param inputs Deephaven Input objects to determine how columns from Deephaven tables get aggregated into
     *               data structures that the provided function can work with.
     * @param batchSize Number of rows from the table to pass through the function before the results are evaluated.
     *                  This can be used for ensuring CPU/GPU is always being used optimally. Note that for training
     *                  models, batch size needs to be equal to the size of the table.
     */
    public Future(PyObject func, int batchSize, Input[] inputs) {

        this.func = func;
        this.batchSize = batchSize;
        this.inputs = inputs;
        this.indexSet = new IndexSet(batchSize);
        this.called = false;
        this.result = null;
    }

    /**
     * Calls the given Python function on the data contained in the Deephaven table at the specified indices. This is
     * where the computations actually happen, as data is both gathered via gather functions and passed through the
     * given Python function here. It gets evaluated once per Future and can then be accessed as many times as needed.
     *
     * @return  PyObject that is the result of applying the given Python function to the gathered dataset.
     */
    public PyObject get() {

        // if this is the first time .get has been called, it has not yet been evaluated. So, evaluate
        // and return results. If this is not the first time, we have already evaluated, so just return results.
        if (!this.called) {

            // create array to hold gathered object. Each element of array corresponds to gathering for that Input
            PyObject[] gathered = new PyObject[this.inputs.length];
            // create a python function caller to call the user-provided function on the gathered data
            PythonFunction<PyObject> funcCaller = new PythonFunction<>(this.func, PyObject.class);
            // for each Input, gather data according to provided gather function and insert into gathered array

            for (int i = 0 ; i < inputs.length ; i++) {
                PyObject thisGathered;
                PythonFunction<IndexSet> gatherCaller = new PythonFunction<>(inputs[i].func, PyObject.class);
                thisGathered = gatherCaller.pyObjectApply(this.indexSet, inputs[i].createColumnSource());
                gathered[i] = thisGathered;
            }

            this.result = funcCaller.pyObjectApply(gathered);
            this.called = true;
        }

        return this.result;
    }
}