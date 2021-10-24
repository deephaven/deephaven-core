package io.deephaven.engine.v2;

import io.deephaven.engine.v2.utils.TrackingMutableRowSet;

interface IncrementalNaturalJoinStateManager {
    long getRightIndex(long slot);
    TrackingMutableRowSet getLeftIndex(long slot);
    String keyString(long slot);
    void checkExactMatch(boolean exactMatch, long leftKeyIndex, long rightSide);
}
