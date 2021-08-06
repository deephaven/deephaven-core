package io.deephaven.integrations.learn;

import io.deephaven.integrations.python.PythonFunction;
import org.jpy.PyObject;

/**
 * Performs a deferred computation on a portion of a table.
 */
public class Future {

    private final PyObject func;
    private final Input[] inputs;
    private final IndexSet indexSet;
    private boolean called;
    private PyObject result;

    /**
     * Creates a new Future.
     *
     * @param func Function that performs computation on gathered data.
     * @param inputs Inputs to the Future computation.
     * @param batchSize Maximum number of rows for deferred computation.
     */
    public Future(PyObject func, int batchSize, Input[] inputs) {

        this.func = func;
        this.inputs = inputs;
        this.indexSet = new IndexSet(batchSize);
        this.called = false;
        this.result = null;
    }

    /**
     * Evaluates the function call once and gets the result as needed.
     *
     * @return  Result of the deferred calculation.
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
                PythonFunction<IndexSet> gatherCaller = new PythonFunction<>(inputs[i].getFunc(), PyObject.class);
                thisGathered = gatherCaller.pyObjectApply(this.indexSet, inputs[i].createColumnSource());
                gathered[i] = thisGathered; }

            this.result = funcCaller.pyObjectApply(gathered);
            this.called = true; }

        return this.result;
    }

    /** Returns this index set. */
    public IndexSet getIndexSet() { return this.indexSet; }
}