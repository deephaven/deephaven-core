package io.deephaven.engine.v2;

import io.deephaven.engine.v2.utils.Index;

public interface CrossJoinStateManager {
    Index getRightIndexFromLeftIndex(long leftIndex);

    Index getRightIndexFromPrevLeftIndex(long leftIndex);

    long getShifted(long index);
    long getPrevShifted(long index);
    long getMasked(long index);
    long getPrevMasked(long index);
}
