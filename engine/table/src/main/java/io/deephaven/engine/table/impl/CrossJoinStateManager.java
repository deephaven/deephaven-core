/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.TrackingRowSet;

public interface CrossJoinStateManager {
    TrackingRowSet getRightRowSetFromLeftIndex(long leftIndex);

    TrackingRowSet getRightRowSetFromPrevLeftIndex(long leftIndex);

    long getShifted(long index);
    long getPrevShifted(long index);
    long getMasked(long index);
    long getPrevMasked(long index);
}
