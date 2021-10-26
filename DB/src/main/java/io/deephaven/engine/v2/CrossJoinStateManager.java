package io.deephaven.engine.v2;

import io.deephaven.engine.v2.utils.TrackingRowSet;

public interface CrossJoinStateManager {
    TrackingRowSet getRightIndexFromLeftIndex(long leftIndex);

    TrackingRowSet getRightIndexFromPrevLeftIndex(long leftIndex);

    long getShifted(long index);
    long getPrevShifted(long index);
    long getMasked(long index);
    long getPrevMasked(long index);
}
