package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.TrackingRowSet;

public interface CrossJoinStateManager {
    TrackingRowSet getRightIndexFromLeftIndex(long leftIndex);

    TrackingRowSet getRightIndexFromPrevLeftIndex(long leftIndex);

    long getShifted(long index);
    long getPrevShifted(long index);
    long getMasked(long index);
    long getPrevMasked(long index);
}
