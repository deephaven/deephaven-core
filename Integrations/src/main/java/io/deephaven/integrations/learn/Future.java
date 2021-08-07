package io.deephaven.integrations.learn;

import io.deephaven.db.tables.Table;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.integrations.python.PythonFunction;
import org.jpy.PyObject;

/**
 * Performs a deferred computation on a portion of a table.
 */
public class Future {

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
    public Future(PyObject func, int batchSize, Input[] inputs, ColumnSource<?>[][] colSet) {

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
    public PyObject get() {

        if (!called) {
            PyObject[] gathered = new PyObject[inputs.length];

            for (int i = 0 ; i < inputs.length ; i++) {
                PythonFunction<IndexSet> gatherCaller = new PythonFunction<>(inputs[i].getFunc(), PyObject.class);
                gathered[i] = gatherCaller.pyObjectApply(indexSet, colSet[i]);
            }

            result = new PythonFunction<>(func, PyObject.class).pyObjectApply(gathered);
            indexSet = null;
            called = true;
        }

        return result;
    }

    /**
     * Getter method for indexSet.
     *
     * @return the current index set.
     */
    public IndexSet getIndexSet() { return indexSet; }
}