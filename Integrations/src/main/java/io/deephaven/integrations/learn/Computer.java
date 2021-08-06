package io.deephaven.integrations.learn;

import org.jpy.PyObject;

/**
 * Instantiates the objects needed for computation using the given indices.
 */
public class Computer {

    private final PyObject func;
    private final int batchSize;
    private final Input[] inputs;
    private Future current;
    private int offset;

    /**
     * Creates a new Computer.
     *
     * @param func Function for performing a computation on these inputs.
     * @param inputs Inputs to this function.
     * @param batchSize Maximum number of rows for deferred computation.
     */
    public Computer(PyObject func, int batchSize, Input ... inputs) {

        if (batchSize <= 0) {
            throw new IllegalArgumentException("Max size must be a strictly positive integer."); }

        if (inputs.length == 0) {
            throw new IllegalArgumentException("Cannot have an empty input list."); }

        this.func = func;
        this.batchSize = batchSize;
        this.inputs = inputs;
        this.current = null;
        this.offset = -1;
    }

    /** Resets this Future to null. */
    public boolean clear() {

        this.current = null;
        return false; // note this only has a return value because functions called in query strings cannot be void
    }

    /**
     * Adds new indices to this future. Will instantiate a new future if needed.
     *
     * @param k Index to be added to this Future's index set.
     * @return  A wrapper for this future with the new index.
     * @throws Exception Cannot add more indices than the maximum number allowed.
     */
    public FutureOffset compute(long k) throws Exception {

        if (this.current == null || this.current.getIndexSet().isFull()) {
            this.current = new Future(this.func, this.batchSize, this.inputs);
            this.offset = -1; }

        this.current.getIndexSet().add(k);
        this.offset += 1;
        return new FutureOffset(this.current, this.offset % this.batchSize);
    }
}
