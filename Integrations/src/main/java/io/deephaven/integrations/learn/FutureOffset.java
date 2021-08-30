package io.deephaven.integrations.learn;

import io.deephaven.base.verify.Require;

/**
 * FutureOffset pairs a deferred calculation with an offset, an index that indicates the location in the calculated
 * result.
 */
public class FutureOffset {

    private Future future;
    private final int offset;

    /**
     * Creates a new FutureOffset.
     *
     * @param future deferred calculation.
     * @param offset location in the calculated result.
     */
    FutureOffset(Future future, int offset) {

        Require.neqNull(future, "future");
        Require.geqZero(offset, "offset");

        this.future = future;
        this.offset = offset;
    }

    /**
     * Gets the offset.
     *
     * @return the offset, indicating where in the calculated result to find the result for a given row index.
     */
    public int getOffset() {
        return offset;
    }

    /**
     * Gets the deferred calculation.
     *
     * @return the deferred calculation.
     */
    public Future getFuture() {
        return future;
    }

    /**
     * Resets the future.
     */
    public void clear() {
        future = null;
    }
}
