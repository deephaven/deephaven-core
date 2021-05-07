package io.deephaven.db.v2;

import io.deephaven.db.v2.utils.Index;

interface IncrementalNaturalJoinStateManager {
    long getRightIndex(long slot);
    Index getLeftIndex(long slot);
    String keyString(long slot);
    void checkExactMatch(boolean exactMatch, long leftKeyIndex, long rightSide);
}
