package io.deephaven.integrations.learn;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnSource;

import java.util.function.Function;

/**
 * Future performs a deferred computation on a portion of a table.
 */
public class Future {

    private final Function<Object[], Object> func;
    private final Input[] inputs;
    private final ColumnSource<?>[][] colSets;
    private final int batchSize;
    private WritableRowSet rowSet;
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
        this.colSets = colSets;
        this.batchSize = batchSize;
        this.rowSet = RowSetFactory.empty();
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
            try {
                Object[] gathered = new Object[inputs.length];

                for (int i = 0; i < inputs.length; i++) {
                    gathered[i] = gather(inputs[i], colSets[i]);
                }

                result = func.apply(gathered);
            } finally {
                rowSet.close();
                rowSet = null;
                called = true;
            }
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
        return input.getGatherFunc().apply(new Object[] {this.rowSet, colSet});
    }

    /**
     * Gets the current row set.
     *
     * @return the current row set.
     */
    RowSet getRowSet() {
        return rowSet;
    }

    /**
     * Add a new row key to those being processed by this future.
     *
     * @param key row key
     */
    void insertRowKey(long key) {
        Assert.assertion(!isFull(), "Attempting to insert into a full Future");
        rowSet.insert(key);
    }

    /**
     * Number of keys being processed by this future.
     *
     * @return number of keys being processed by this future.
     */
    long size() {
        return rowSet.size();
    }

    /**
     * Returns true if this future is full of keys to process; false otherwise. The future is full of keys if the size
     * is equal to the batch size.
     *
     * @return true if this future is full of keys to process; false otherwise.
     */
    boolean isFull() {
        return size() >= batchSize;
    }
}
