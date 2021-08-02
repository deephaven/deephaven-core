package io.deephaven.integrations.learn;

import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;

/**
 * This class provides a way to keep track of table indices that have been added or modified. It implements the Java
 * Iterable and thus enables zero-copy element access.
 *
 * This is the third layer of this design, as an IndexSet is only ever instantiated by a Future, itself instantiated
 * by a Computer.
 */

public class IndexSet implements Iterable<Long> {

    int current;
    final int maxSize;
    long[] idx;

    /**
     * Constructor for IndexSet. Allocates memory for a long array of size maxSize and sets the ticker to -1.
     *
     * @param maxSize maximum allowed size of this index set.
     */
    public IndexSet(int maxSize) {

        // first verify that maxSize is a strictly positive integer
        if (maxSize <= 0) {
            throw new IllegalArgumentException("Max size must be a strictly positive integer.");
        }
        this.current = -1;
        this.maxSize = maxSize;
        this.idx = new long[maxSize];
    }


    /**
     * Resets this ticker to -1 and clears this index set.
     */
    void clear() {
        this.current = -1;
        this.idx = new long[this.maxSize];
    }

    /**
     * Computes and returns the current size of this index set.
     *
     * @return  The current size of this index set.
     */
    public int size() { return this.current + 1; }

    /**
     * Determines whether this index set has reached its maximum allowable size.
     *
     * @return  Boolean indicating whether this index set has been filled.
     */
    boolean isFull() { return this.size() >= this.idx.length; }

    /**
     * Adds indices provided as arguments to this index set if allowed.
     *
     * @param k Index to be added to this index set.
     * @throws Exception Cannot add more indices to the index set than the maximum size allowed.
     */
    void add(long k) throws Exception {
        if (this.current == this.idx.length) {
            throw new Exception("Adding more indices than can fit.");
        }
        this.current += 1;
        this.idx[this.current] = k;
    }

    @Override
    public PrimitiveIterator.OfLong iterator() {

        return new PrimitiveIterator.OfLong() {
            int i = -1;
            @Override
            // return the next element in the index set if possible
            public long nextLong() {
                if (!hasNext()) {
                    throw new NoSuchElementException("There are no more elements in the index set.");
                }
                i++;
                return IndexSet.this.idx[i];
            }

            @Override
            // whether there are any elements left to be traversed
            public boolean hasNext() {
                return i < IndexSet.this.current;
            }
        };
    }
}