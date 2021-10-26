package io.deephaven.engine.v2;

import io.deephaven.engine.v2.utils.RowSet;

interface IncrementalNaturalJoinStateManager {
    long getRightIndex(long slot);
    RowSet getLeftIndex(long slot);
    String keyString(long slot);
    void checkExactMatch(boolean exactMatch, long leftKeyIndex, long rightSide);
}
