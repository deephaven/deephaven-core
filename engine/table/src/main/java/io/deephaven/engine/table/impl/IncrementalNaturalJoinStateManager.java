package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.RowSet;

interface IncrementalNaturalJoinStateManager {
    long getRightIndex(long slot);
    RowSet getLeftIndex(long slot);
    String keyString(long slot);
    void checkExactMatch(boolean exactMatch, long leftKeyIndex, long rightSide);
}
