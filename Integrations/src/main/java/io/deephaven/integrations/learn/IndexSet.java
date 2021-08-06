package io.deephaven.integrations.learn;

import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;

/**
 * Stores indices from a table and provides an iterator over those indices.
 */
public class IndexSet implements Iterable<Long> {

    private int current;
    private final int maxSize;
    private final long[] idx;

    /**
     * Creates a new IndexSet.
     *
     * @param maxSize Maximum allowed size of this index set.
     */
    public IndexSet(int maxSize) {

        if (maxSize <= 0) {
            throw new IllegalArgumentException("Max size must be a strictly positive integer."); }

        this.current = -1;
        this.maxSize = maxSize;
        this.idx = new long[maxSize];
    }

    /**
     * Determines whether this index set has reached its maximum allowable size.
     *
     * @return  Whether this index set has been filled.
     */
    boolean isFull() { return this.getSize() >= this.idx.length; }

    /**
     * Adds indices provided as arguments to this index set if allowed.
     *
     * @param k Index to be added to this index set.
     * @throws Exception Cannot add more indices to the index set than the maximum size allowed.
     */
    void add(long k) throws Exception {

        if (this.current == this.idx.length) {
            throw new Exception("Adding more indices than can fit."); }

        this.current += 1;
        this.idx[this.current] = k;
    }

    @Override
    public PrimitiveIterator.OfLong iterator() {

        return new PrimitiveIterator.OfLong() {
            int i = -1;
            @Override
            public long nextLong() {

                if (!hasNext()) {
                    throw new NoSuchElementException("There are no more elements in the index set."); }

                i++;
                return IndexSet.this.idx[i];
            }

            @Override
            public boolean hasNext() {
                return i < IndexSet.this.current;
            }
        };
    }

    /** Returns this current size. */
    public int getSize() { return this.current + 1; }

    /** Returns this maximum size. */
    public int getMaxSize() { return this.maxSize; }
}