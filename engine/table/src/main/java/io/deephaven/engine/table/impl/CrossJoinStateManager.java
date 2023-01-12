/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.TrackingRowSet;

public interface CrossJoinStateManager {
    TrackingRowSet getRightRowSetFromLeftRow(long leftIndex);

    TrackingRowSet getRightRowSetFromPrevLeftRow(long leftIndex);

    long getShifted(long index);
    long getPrevShifted(long index);
    long getMasked(long index);
    long getPrevMasked(long index);

    /**
     * If our result is a leftOuterJoin, which means that for each unmatched left row we produce one row of RHS output,
     * with null values for the columns to add.
     */
    boolean leftOuterJoin();
}
