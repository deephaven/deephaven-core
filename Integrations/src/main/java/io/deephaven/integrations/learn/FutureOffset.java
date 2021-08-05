package io.deephaven.integrations.learn;

/**
 * This class wraps a Future together with an immutable offset for use in scattering functionality.
 */

public class FutureOffset {

    final Future future;
    final int offset;

    /**
     * Constructor for FutureOffset.
     *
     * @param future Future to hold with FutureOffset
     * @param offset Offset to use in scattering
     */
    public FutureOffset(Future future, int offset) {

        this.future = future;
        this.offset = offset;
    }

    /**
     * Getter method for offset.
     *
     * @return Integer offset
     */
    public int getOffset() { return this.offset; }

    /**
     * Getter method for Future.
     *
     * @return Future.
     */
    public Future getFuture() { return this.future; }
}
