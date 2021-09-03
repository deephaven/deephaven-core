package io.deephaven.engine.v2;

import io.deephaven.engine.structures.rowset.Index;

interface IncrementalNaturalJoinStateManager {
    long getRightIndex(long slot);
    Index getLeftIndex(long slot);
    String keyString(long slot);
    void checkExactMatch(boolean exactMatch, long leftKeyIndex, long rightSide);
}
