package io.deephaven.integrations.learn;

import io.deephaven.db.tables.Table;
import org.jpy.PyObject;

/**
 * This class creates and adds to this index set when indices are updated or added, and instantiates the Future needed
 * to perform computations on the data at those indices.
 *
 * This is the first layer of this design, as it directly interacts with the dataset to retrieve indices that have been
 * updated or added.
 */

public class Computer {

    final PyObject func;
    final int batchSize;
    final Input[] inputs;
    Future current;
    int offset;

    /**
     * Constructor for Computer. Initializes fields that are needed to instantiate a Future.
     *
     * @param func Python function that the user wants to use on data from a Deephaven table.
     * @param inputs Deephaven Input objects that determine which columns of data get passed to func.
     * @param batchSize Maximum number of rows to perform computations on at once.
     */
    public Computer(PyObject func, int batchSize, Input ... inputs) {

        if (batchSize <= 0) {
            throw new IllegalArgumentException("Max size must be a strictly positive integer.");
        }

        if (inputs.length == 0) {
            throw new IllegalArgumentException("Cannot have an empty input list.");
        }

        this.func = func;
        this.batchSize = batchSize;
        this.inputs = inputs;
        this.current = null;
        this.offset = -1;
    }

    /**
     * Resets this Future to null.
     */
    public boolean clear() {

        this.current = null;
        return false;
    }

    /**
     * This method will instantiate a Future if there is not a currently existing one (first call to .compute), or if
     * the current one has an index set that is full. Then, it will add indices for computation to this index set, and
     * defer the actual computation to the .get method of the associated Future.
     *
     * @param k Index to be added to this Future's index set.
     * @return  A Future with the k index added to its index set.
     * @throws Exception Cannot add more indices than the maximum number allowed.
     */
    public FutureOffset compute(long k) throws Exception {

        // if current has not been created or its index set is full, create a new one and append appropriate indices
        // and return. Else, just append appropriate indices and return.
        if (this.current == null || this.current.indexSet.isFull()) {
            this.current = new Future(this.func, this.batchSize, this.inputs);
            this.offset = -1;
        }

        this.current.indexSet.add(k);
        this.offset += 1;
        return new FutureOffset(this.current, this.offset % this.batchSize);
    }
}
