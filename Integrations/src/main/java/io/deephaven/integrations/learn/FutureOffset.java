package io.deephaven.integrations.learn;

import io.deephaven.base.verify.Require;
import org.jpy.PyObject;

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

        Require.neqNull(future, "future");
        Require.neqNull(offset, "offset");

        Require.geqZero(offset, "offset");

        this.future = future;
        this.offset = offset;
    }

    /**
     * Gets the offset.
     *
     * @return the offset, indicating where in the calculated result to find the result for a given row index.
     */
    public int getOffset() { return offset; }

    /** Gets the result of the deferred calculation.
     *
     * @return the result of deferred calculation.
     */
    public Object getFutureGet() { return future.get(); }
}
