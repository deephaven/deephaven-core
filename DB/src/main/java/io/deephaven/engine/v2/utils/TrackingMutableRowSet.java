/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.utils;

import org.jetbrains.annotations.NotNull;

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
    void initializePreviousValue();

    /**
     * <p>
     * Convert the supplied {@link RowSet} reference to a TrackingMutableRowSet with the minimal set of casts or
     * operations.
     * <p>
     * If {@code rowSet} is a TrackingMutableRowSet, it will be returned with a cast.
     * <p>
     * If {@code rowSet} is a {@link MutableRowSet}, tracking will be {@link #tracking() enabled}.
     * <p>
     * If {@code rowSet} is not mutable, it will be {@link #clone() cloned}.
     *
     * @param rowSet The {@link RowSet} reference to convert
     * @return {@code rowSet}, or a new TrackingMutableRowSet backed by the same contents as {@code rowSet}
     */
    static TrackingMutableRowSet convert(@NotNull final RowSet rowSet) {
        if (rowSet instanceof TrackingMutableRowSet) {
            return (TrackingMutableRowSet) rowSet;
        }
        if (rowSet instanceof MutableRowSet) {
            return ((MutableRowSet) rowSet).tracking();
        }
        return rowSet.clone().tracking();
    }
}
