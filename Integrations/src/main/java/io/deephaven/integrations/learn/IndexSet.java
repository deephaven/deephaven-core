package io.deephaven.integrations.learn;

import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;

/**
 * IndexSet stores indices from a table and provides an iterator over those indices.
 */
class IndexSet implements Iterable<Long> {

    private int current;
    private final long[] idx;

    /**
     * Creates a new IndexSet.
     *
     * @param maxSize maximum allowed size of this index set.
     */
    IndexSet(int maxSize) {

        if (maxSize <= 0) {
            throw new IllegalArgumentException("Max size must be a strictly positive integer.");
        }

        this.current = -1;
        this.idx = new long[maxSize];
    }

    /**
     * Determines whether this index set has reached its maximum allowable size.
     *
     * @return whether this index set has been filled.
     */
    boolean isFull() {
        return getSize() >= idx.length;
    }

    /**
     * Adds an index to this index set.
     *
     * @param k index to be added to this index set.
     */
    void add(long k) {

        if (isFull()) {
            throw new IndexOutOfBoundsException("Adding more indices than can fit.");
        }

        current += 1;
        idx[current] = k;
    }

    @Override
    public PrimitiveIterator.OfLong iterator() {

        return new PrimitiveIterator.OfLong() {
            int i = -1;

            @Override
            public long nextLong() {

                if (!hasNext()) {
                    throw new NoSuchElementException("There are no more elements in the index set.");
                }

                i++;
                return IndexSet.this.idx[i];
            }

            @Override
            public boolean hasNext() {
                return i < IndexSet.this.current;
            }
        };
    }

    /**
     * Gets the number of elements in the index set.
     *
     * @return number of elements in the index set.
     */
    public int getSize() {
        return current + 1;
    }
}
