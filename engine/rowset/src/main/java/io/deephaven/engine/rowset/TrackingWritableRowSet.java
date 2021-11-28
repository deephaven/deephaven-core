/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.rowset;

/**
 * Tracking, writable {@link RowSet}.
 */
public interface TrackingWritableRowSet extends WritableRowSet, TrackingRowSet {

    /**
     * Initializes our previous value from the current value.
     * <p>
     * This call is used by operations that manipulate a TrackingWritableRowSet while constructing it, but need to set
     * the state at the end of the initial operation to the current state.
     * <p>
     * Calling this in other circumstances will yield undefined results.
     */
    void initializePreviousValue();
}
