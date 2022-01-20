package io.deephaven.csv.parsers;

import io.deephaven.csv.densestorage.DenseStorageReader;
import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.tokenization.RangeTests;
import io.deephaven.csv.util.CsvReaderException;

/**
 * This class is used to hold the underlying {@link DenseStorageReader} plus some associated helper information (an
 * allocated {@link ByteSlice} for slice storage, plus a couple helpful statistics like {@link #numConsumed} and
 * {@link #isExhausted}.
 */
public final class IteratorHolder {
    /**
     * The {@link DenseStorageReader} for the input text.
     */
    private final DenseStorageReader dsr;
    /**
     * Storage for our reusable byte slice. Data inside it is valid after a call to tryMoveNext() returns true, in the
     * case where hasBytes has been set to true.
     */
    private final ByteSlice bs = new ByteSlice();
    /**
     * Number of successful calls so far to tryMoveNext (i.e. those that returned true).
     */
    private long numConsumed = 0;
    /**
     * Valid anytime after the first call to tryMoveNext(), but not before.
     */
    private boolean isExhausted = false;

    /**
     * Constructor.
     */
    public IteratorHolder(DenseStorageReader dsr) {
        this.dsr = dsr;
    }

    /**
     * Try to advance to the next (or very first) item.
     * 
     * @return true if we were able to advance, and set {@link IteratorHolder#bs} to valid text. Otherwise false.
     */
    public boolean tryMoveNext() throws CsvReaderException {
        isExhausted = !dsr.tryGetNextSlice(bs);
        if (isExhausted) {
            return false;
        }
        ++numConsumed;
        return true;
    }

    /**
     * Getter for the byte slice.
     */
    public ByteSlice bs() {
        return bs;
    }

    /**
     * Number of items we've consumed so far. This is the number of times {@link #tryMoveNext} has been called and
     * returned true.
     * 
     * @return The number of items we've consumed so far
     */
    public long numConsumed() {
        return numConsumed;
    }

    /**
     * Is the iteration exhausted? This is set to true when {@link #tryMoveNext} is called and returns false.
     * 
     * @return Whether the iteration is exhausted.
     */
    public boolean isExhausted() {
        return isExhausted;
    }
}
