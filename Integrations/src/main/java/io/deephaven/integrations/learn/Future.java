package io.deephaven.integrations.learn;

import io.deephaven.db.v2.sources.ColumnSource;
import java.util.function.Function;

/**
 * Future performs a deferred computation on a portion of a table.
 */
public class Future {

    private final Function<Object[], Object> func;
    private final ColumnSource<?>[][] colSets;
    private final Input[] inputs;
    private IndexSet indexSet;
    private boolean called;
    private Object result;

    /**
     * Creates a new Future.
     *
     * @param func function that performs computation on gathered data.
     * @param inputs inputs to the Future computation.
     * @param batchSize maximum number of rows for deferred computation.
     */
    Future(Function<Object[], Object> func, Input[] inputs, ColumnSource<?>[][] colSets, int batchSize) {

        this.func = func;
        this.inputs = inputs;
        this.indexSet = new IndexSet(batchSize);
        this.colSets = colSets;
        this.called = false;
        this.result = null;
    }

    /**
     * Gets the result of the deferred calculation. The calculation is performed at most once, and results are cached.
     *
     * @return result of the deferred calculation.
     */
    public Object get() {

        if (!called) {
            Object[] gathered = new Object[inputs.length];

            for (int i = 0; i < inputs.length; i++) {
                gathered[i] = gather(inputs[i], colSets[i]);
            }

            result = func.apply(gathered);
            indexSet = null;
            called = true;
        }

        return result;
    }

    /**
     * Computes the result of applying the gather function to the given input.
     *
     * @param input input that contains the gather function and the column names to gather.
     * @param colSet set of column sources from which to extract data.
     * @return gathered data
     */
    Object gather(Input input, ColumnSource<?>[] colSet) {
        return input.getGatherFunc().apply(new Object[] {this.indexSet, colSet});
    }

    /**
     * Gets the current index set.
     *
     * @return the current index set.
     */
    IndexSet getIndexSet() {
        return indexSet;
    }
}
