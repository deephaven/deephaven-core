//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.NaturalJoinType;
import io.deephaven.engine.rowset.RowSet;

public interface IncrementalNaturalJoinStateManager {
    long getRightIndex(int slot);

    RowSet getLeftIndex(int slot);

    String keyString(int slot);

    void checkExactMatch(NaturalJoinType joinType, long leftKeyIndex, long rightSide);
}
