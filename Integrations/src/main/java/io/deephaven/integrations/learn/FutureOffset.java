package io.deephaven.integrations.learn;

/**
 * FutureOffset pairs a deferred calculation with an offset, an index that indicates the location in the calculated result.
 */
public class FutureOffset {

    private final Future future;
    private final int offset;

    /**
     * Creates a new FutureOffset.
     *
     * @param future    deferred calculation.
     * @param offset    location in the calculated result.
     */
    public FutureOffset(Future future, int offset) {

        this.future = future;
        this.offset = offset;
    }

    /**
     * Getter method for offset.
     *
     * @return the offset, indicating where in the calculated result to find the result for a given row index.
     */
    public int getOffset() { return offset; }

    /** Getter method for future.
     *
     * @return the current future.
     */
    public Future getFuture() { return future; }
}
