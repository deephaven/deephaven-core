package io.deephaven.integrations.learn;

import io.deephaven.db.tables.Table;
import io.deephaven.db.v2.sources.ColumnSource;
import org.jpy.PyObject;

/**
 * Passes indices to Future for deferred calculation.
 */
public class Computer {

    private final PyObject modelFunc;
    private final int batchSize;
    private final Input[] inputs;
    private final ColumnSource<?>[][] colSet;
    private Future current;
    private int offset;

    /**
     * Creates a new Computer.
     *
     * @param modelFunc     function to use for AI training / prediction on the given inputs.
     * @param inputs        inputs to the model function.
     * @param batchSize     maximum number of rows for deferred computation.
     */
    public Computer(Table table, PyObject modelFunc, int batchSize, Input ... inputs) {

        if (modelFunc == null) {
            throw new IllegalArgumentException("Model function cannot be null.");
        }

        if (batchSize <= 0) {
            throw new IllegalArgumentException("Max size must be a strictly positive integer.");
        }

        if (inputs.length == 0) {
            throw new IllegalArgumentException("Cannot have an empty input list.");
        }

        this.modelFunc = modelFunc;
        this.batchSize = batchSize;
        this.inputs = inputs;

        this.colSet = new ColumnSource[this.inputs.length][];
        for (int i = 0 ; i < this.inputs.length ; i++) {
            this.colSet[i] = inputs[i].createColumnSource(table);
        }

        this.current = null;
        this.offset = -1;
    }

    /**
     * Resets the current future to clear memory for the next one.
     *
     * @return always false, because functions used in query strings cannot return nothing.
     */
    public boolean clear() {

        current = null;
        return false;
    }

    /**
     * Adds new row indices to the calculation.
     *
     * @param k     index to be added to this Future's index set.
     * @return      future offset that combines this future with the relevant row index to access result.
     */
    public FutureOffset compute(long k) {

        if (current == null || current.getIndexSet().isFull()) {
            current = new Future(modelFunc, batchSize, inputs, colSet);
            offset = -1;
        }

        current.getIndexSet().add(k);
        offset += 1;
        return new FutureOffset(current, offset % batchSize);
    }
}
