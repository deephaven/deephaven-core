package io.deephaven.engine.v2;

import io.deephaven.engine.v2.utils.TrackingMutableRowSet;

public interface CrossJoinStateManager {
    TrackingMutableRowSet getRightIndexFromLeftIndex(long leftIndex);

    TrackingMutableRowSet getRightIndexFromPrevLeftIndex(long leftIndex);

    long getShifted(long index);
    long getPrevShifted(long index);
    long getMasked(long index);
    long getPrevMasked(long index);
}
