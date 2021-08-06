package io.deephaven.integrations.learn;

/**
 * Wrapper for a Future together with an immutable offset for use in scattering functionality.
 */
public class FutureOffset {

    private final Future future;
    private final int offset;

    /**
     * Creates a new FutureOffset.
     *
     * @param future Future to hold with FutureOffset
     * @param offset Offset to use in scattering
     */
    public FutureOffset(Future future, int offset) {

        this.future = future;
        this.offset = offset;
    }

    /** Returns this offset. */
    public int getOffset() { return this.offset; }

    /** Returns this future. */
    public Future getFuture() { return this.future; }
}
