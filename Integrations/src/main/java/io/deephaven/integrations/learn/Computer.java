package io.deephaven.integrations.learn;

import io.deephaven.base.verify.Require;
import io.deephaven.db.tables.Table;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.integrations.python.PythonFunctionCaller;
import org.jpy.PyObject;

import java.util.ArrayList;
import java.util.function.Function;

/**
 * Computer creates a new deferred calculation plus an offset (FutureOffset) every time computer is called.
 */
public class Computer {

    private final Function<Object[], Object> modelFunc;
    private final ColumnSource<?>[][] colSets;
    private final int batchSize;
    private final Input[] inputs;
    private Future current;
    private final ArrayList<FutureOffset> futureOffsets;
    private int offset;

    /**
     * Creates a new Computer.
     *
     * @param modelFunc python function to call on the given inputs from a table.
     * @param inputs inputs to the model function.
     * @param batchSize maximum number of rows for each deferred computation.
     */
    public Computer(Table table, PyObject modelFunc, Input[] inputs, int batchSize) {
        this(table, new PythonFunctionCaller(Require.neqNull(modelFunc, "modelFunc")), inputs, batchSize);
    }

    /**
     * Creates a new Computer.
     *
     * @param modelFunc python function to call on the given inputs from a table.
     * @param inputs inputs to the model function.
     * @param batchSize maximum number of rows for each deferred computation.
     */
    public Computer(Table table, Function<Object[], Object> modelFunc, Input[] inputs, int batchSize) {

        Require.neqNull(table, "table");
        Require.neqNull(modelFunc, "modelFunc");
        Require.neqNull(inputs, "inputs");
        Require.elementsNeqNull(inputs, "inputs");
        Require.gtZero(batchSize, "batchSize");

        this.modelFunc = modelFunc;
        this.batchSize = batchSize;
        this.inputs = inputs;

        this.colSets = new ColumnSource[this.inputs.length][];

        for (int i = 0; i < this.inputs.length; i++) {
            this.colSets[i] = inputs[i].createColumnSource(table);
        }

        this.current = null;
        this.futureOffsets = new ArrayList<>(batchSize);
        this.offset = -1;
    }

    /**
     * Resets the current future after each set of calculations.
     *
     * @return always false, because functions used in query strings cannot return nothing.
     */
    public boolean clear() {

        for (FutureOffset futureOffset : futureOffsets) {
            futureOffset.clear();
        }

        futureOffsets.clear();
        current = null;
        return false;
    }

    /**
     * Adds new row indices to be used in the deferred calculation.
     *
     * @param k index to be added to the current index set.
     * @return future offset that combines a future with the relevant row index to access result.
     */
    public FutureOffset compute(long k) {

        if (current == null || current.getIndexSet().isFull()) {
            current = new Future(modelFunc, inputs, colSets, batchSize);
            offset = -1;
        }

        current.getIndexSet().add(k);
        offset += 1;

        final FutureOffset fo = new FutureOffset(current, offset);
        futureOffsets.add(fo);

        return fo;
    }

    /**
     * Gets the current future.
     *
     * @return the current future.
     */
    Future getFuture() {
        return current;
    }
}
