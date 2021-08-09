package io.deephaven.integrations.learn;

import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.integrations.python.PythonFunctionCaller;
import org.jpy.PyObject;

/**
 * Future performs a deferred computation on a portion of a table.
 */
class Future {

    private final PyObject func;
    private final Input[] inputs;
    private IndexSet indexSet;
    private final ColumnSource<?>[][] colSet;
    private boolean called;
    private PyObject result;

    /**
     * Creates a new Future.
     *
     * @param func          function that performs computation on gathered data.
     * @param inputs        inputs to the Future computation.
     * @param batchSize     maximum number of rows for deferred computation.
     */
    Future(PyObject func, int batchSize, Input[] inputs, ColumnSource<?>[][] colSet) {

        this.func = func;
        this.inputs = inputs;
        this.indexSet = new IndexSet(batchSize);
        this.colSet = colSet;
        this.called = false;
        this.result = null;
    }

    /**
     * Gets the result of the deferred calculation. The calculation is performed at most once, and results are cached.
     *
     * @return result of the deferred calculation.
     */
    PyObject get() {

        if (!called) {

            PyObject[] gathered = new PyObject[inputs.length];
            for (int i = 0; i < inputs.length ; i++) {
                gathered[i] = gather(inputs[i], indexSet, colSet[i]);
            }

            result = new PythonFunctionCaller(func).apply(gathered);
            indexSet = null;
            called = true;
        }

        return result;
    }

    /**
     * Computes the result of applying the gather function to the given input.
     *
     * @param input     input that contains the gather function and the column names to gather.
     * @param indexSet  gives the indices for which we want to gather data.
     * @param colSet    set of column sources from which to extract data.
     * @return gathered data
     */
    PyObject gather(Input input, IndexSet indexSet, ColumnSource<?>[] colSet) {
        return input.getGatherCaller().apply(indexSet, colSet);
    }

    /**
     * Gets the current index set.
     *
     * @return the current index set.
     */
    IndexSet getIndexSet() { return indexSet; }
}