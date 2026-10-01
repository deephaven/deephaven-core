//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.NaturalJoinType;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.util.QueryConstants;

public interface IncrementalNaturalJoinStateManager {
    /**
     * The right state of a hash slot that has never held a key.
     */
    long EMPTY_RIGHT_STATE = QueryConstants.NULL_LONG;
    /**
     * The right state of a hash slot whose key was deleted (only produced by the both-incremental state manager).
     */
    long TOMBSTONE_RIGHT_STATE = RowSet.NULL_ROW_KEY - 1;
    /**
     * Right states at or below this value encode a location in the right-side duplicate row sets: {@code
     * FIRST_DUPLICATE} maps to location 0, {@code FIRST_DUPLICATE - 1} to location 1, and so on. A right state above
     * this value is a single right row key, {@link RowSet#NULL_ROW_KEY} for a key with no right row, or the tombstone.
     */
    long FIRST_DUPLICATE = TOMBSTONE_RIGHT_STATE - 1;

    /**
     * @param rightState a right state as stored in a hash slot
     * @return whether the state encodes a duplicate row set location, rather than a row key or one of the no-right-row
     *         markers ({@link RowSet#NULL_ROW_KEY}, {@link #TOMBSTONE_RIGHT_STATE}, {@link #EMPTY_RIGHT_STATE})
     */
    static boolean isDuplicateRightState(final long rightState) {
        return rightState <= FIRST_DUPLICATE && rightState != EMPTY_RIGHT_STATE;
    }

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
     * Whether turning a slot's single right row into a duplicate set changes what its left rows must observe. The join
     * types that reject duplicates must record the slot so the error is raised while processing the modified slots; the
     * others only when the newly selected right row differs from the one the left rows already hold.
     *
     * @param duplicates the duplicate set, after the new key has been added
     * @param existingRightRowKey the slot's right row key before the duplicate set was created
     * @param joinType the join type
     * @return whether the slot must be recorded as changed in the modified slot tracker
     */
    default boolean duplicateCreationChangesState(final WritableRowSet duplicates, final long existingRightRowKey,
            final NaturalJoinType joinType) {
        return joinType == NaturalJoinType.ERROR_ON_DUPLICATE || joinType == NaturalJoinType.EXACTLY_ONE_MATCH
                || getRightRowKeyFromDuplicates(duplicates, joinType) != existingRightRowKey;
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
