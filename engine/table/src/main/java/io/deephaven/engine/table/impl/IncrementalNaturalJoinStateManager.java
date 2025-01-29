//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.NaturalJoinType;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;

public interface IncrementalNaturalJoinStateManager {
    long getRightIndex(int slot);

    RowSet getRightRowSet(int slot);

    RowSet getLeftRowSet(int slot);

    String keyString(int slot);

    void checkExactMatch(NaturalJoinType joinType, long leftKeyIndex, long rightSide);

    /**
     * Given the join type, return the correct row key for the set of duplicate RHS rows.
     */
    default long getRightRowKeyFromDuplicates(final WritableRowSet duplicates, final NaturalJoinType joinType) {
        if (joinType == NaturalJoinType.LAST_MATCH) {
            return duplicates.lastRowKey();
        }
        return duplicates.firstRowKey();
    }

    /**
     * Add a key to the RHS duplicate rowset, following the rules for NaturalJoinType to return the new row key for this
     * set *AFTER* the addition.
     */
    default long addRightRowKeyToDuplicates(final WritableRowSet duplicates, final long keyToRemove,
            final NaturalJoinType joinType) {
        duplicates.insert(keyToRemove);
        return getRightRowKeyFromDuplicates(duplicates, joinType);
    }

    /**
     * Remove the key from the RHS duplicate rowset, following the rules for NaturalJoinType to return the original row
     * key for this set *BEFORE* the removal.
     */
    default long removeRightRowKeyFromDuplicates(final WritableRowSet duplicates, final long keyToRemove,
            final NaturalJoinType joinType) {
        final long originalRowKey = getRightRowKeyFromDuplicates(duplicates, joinType);
        duplicates.remove(keyToRemove);
        return originalRowKey;
    }
}
