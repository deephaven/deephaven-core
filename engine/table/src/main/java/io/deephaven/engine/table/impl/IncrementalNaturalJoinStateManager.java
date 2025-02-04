//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.NaturalJoinType;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;

public interface IncrementalNaturalJoinStateManager {
    long getRightRowKey(int slot);

    RowSet getRightRowSet(int slot);

    RowSet getLeftRowSet(int slot);

    String keyString(int slot);

    void checkExactMatch(long leftKeyIndex, long rightSide);

    /**
     * Given the join type, return the correct row key from the set of RHS duplicates.
     */
    default long getRightRowKeyFromDuplicates(final WritableRowSet duplicates, final NaturalJoinType joinType) {
        if (joinType == NaturalJoinType.LAST_MATCH) {
            return duplicates.lastRowKey();
        }
        return duplicates.firstRowKey();
    }

    /**
     * Add a key to the RHS duplicates, return the appropriate row key from this set *AFTER* the addition.
     */
    default long addRightRowKeyToDuplicates(final WritableRowSet duplicates, final long keyToRemove,
            final NaturalJoinType joinType) {
        duplicates.insert(keyToRemove);
        return getRightRowKeyFromDuplicates(duplicates, joinType);
    }

    /**
     * Remove the key from the RHS duplicates, return the appropriate row key from this set *BEFORE* the removal.
     */
    default long removeRightRowKeyFromDuplicates(final WritableRowSet duplicates, final long keyToRemove,
            final NaturalJoinType joinType) {
        final long originalRowKey = getRightRowKeyFromDuplicates(duplicates, joinType);
        duplicates.remove(keyToRemove);
        return originalRowKey;
    }

    /**
     * Shift a key in the RHS duplicate row set.
     */
    default void shiftOneKey(WritableRowSet duplicates, long shiftedKey, long shiftDelta) {
        final long sizeBefore = duplicates.size();
        duplicates.remove(shiftedKey - shiftDelta);
        duplicates.insert(shiftedKey);
        Assert.eq(duplicates.size(), "duplicates.size()", sizeBefore, "sizeBefore");
    }
}
