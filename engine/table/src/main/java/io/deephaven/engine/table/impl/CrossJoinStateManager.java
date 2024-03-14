//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.TrackingRowSet;

public interface CrossJoinStateManager {
    TrackingRowSet getRightRowSetFromLeftRow(long leftIndex);

    TrackingRowSet getRightRowSetFromPrevLeftRow(long leftIndex);

    long getShifted(long rowKey);

    long getPrevShifted(long rowKey);

    long getMasked(long rowKey);

    long getPrevMasked(long rowKey);

    /**
     * If our result is a leftOuterJoin, which means that for each unmatched left row we produce one row of RHS output,
     * with null values for the columns to add.
     */
    boolean leftOuterJoin();
}
