/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.utils;

import io.deephaven.configuration.Configuration;

/**
 * Tracking, mutable {@link RowSet}.
 */
public interface TrackingMutableRowSet extends MutableRowSet, TrackingRowSet {

    /**
     * Initializes our previous value from the current value.
     *
     * This call is used by operations that manipulate a RowSet while constructing it, but need to set the state at the
     * end of the initial operation to the current state.
     *
     * Calling this in other circumstances will yield undefined results.
     */
    void initializePreviousValue(); // TODO-RWC: I think this can be eliminated
}
