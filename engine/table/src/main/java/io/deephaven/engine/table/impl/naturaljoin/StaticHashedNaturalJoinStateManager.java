//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.naturaljoin;

import io.deephaven.api.NaturalJoinType;
import io.deephaven.engine.exceptions.DuplicateRightKeyException;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.JoinControl;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.StaticNaturalJoinStateManager;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.table.impl.util.ContiguousWritableRowRedirection;
import io.deephaven.engine.table.impl.util.LongColumnSourceWritableRowRedirection;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.engine.table.impl.util.WritableRowRedirectionLockFree;

import java.util.function.LongUnaryOperator;

/**
 * The parts the static natural join state managers share: building a row redirection from a data index table's groups,
 * and reporting a duplicate right key. {@link StaticKeyIdNaturalJoinStateManager} builds its hash table from the left
 * table, and {@link StaticKeyLongValueNaturalJoinStateManager} from the right.
 */
public abstract class StaticHashedNaturalJoinStateManager extends StaticNaturalJoinStateManager {

    protected StaticHashedNaturalJoinStateManager(
            ColumnSource<?>[] keySourcesForErrorMessages,
            NaturalJoinType joinType,
            boolean addOnly) {
        super(keySourcesForErrorMessages, joinType, addOnly);
    }

    /**
     * @param keySourcesFromLeftTable whether {@code keySourcesForErrorMessages} are columns of the left table (as when
     *        building from redirections) rather than columns of the data index table (as when building from hash
     *        slots); determines which keyspace error row keys are produced in
     */
    protected WritableRowRedirection buildIndexedRowRedirection(QueryTable leftTable,
            RowSet indexTableRowSet, LongUnaryOperator groupPositionToRightSide, ColumnSource<RowSet> leftRowSets,
            JoinControl.RedirectionType redirectionType, boolean keySourcesFromLeftTable) {
        final int rowSetCount = indexTableRowSet.intSize();
        switch (redirectionType) {
            case Contiguous: {
                if (!leftTable.isFlat()) {
                    throw new IllegalStateException("Left table is not flat for contiguous row redirection build!");
                }
                // we can use an array, which is perfect for a small enough flat table
                final long[] innerIndex = new long[leftTable.intSize("contiguous redirection build")];
                try (final RowSet.Iterator indexKeyIt = indexTableRowSet.iterator()) {
                    for (int ii = 0; ii < rowSetCount; ++ii) {
                        final long indexRowKey = indexKeyIt.nextLong();
                        final long rightSide = groupPositionToRightSide.applyAsLong(ii);
                        if (rightSide == NO_RIGHT_ENTRY_VALUE) {
                            checkExactMatchForGroup(keySourcesFromLeftTable, indexTableRowSet, leftRowSets, ii);
                        }
                        final RowSet leftRowSetForKey = leftRowSets.get(indexRowKey);
                        leftRowSetForKey.forAllRowKeys((long ll) -> innerIndex[(int) ll] = rightSide);
                    }
                }
                return new ContiguousWritableRowRedirection(innerIndex);
            }
            case Sparse: {
                final LongSparseArraySource sparseRedirections = new LongSparseArraySource();

                try (final RowSet.Iterator indexKeyIt = indexTableRowSet.iterator()) {
                    for (int ii = 0; ii < rowSetCount; ++ii) {
                        final long indexRowKey = indexKeyIt.nextLong();
                        final long rightSide = groupPositionToRightSide.applyAsLong(ii);

                        if (rightSide == NO_RIGHT_ENTRY_VALUE) {
                            checkExactMatchForGroup(keySourcesFromLeftTable, indexTableRowSet, leftRowSets, ii);
                        } else {
                            final RowSet leftRowSetForKey = leftRowSets.get(indexRowKey);
                            leftRowSetForKey.forAllRowKeys((long ll) -> sparseRedirections.set(ll, rightSide));
                        }
                    }
                }
                return new LongColumnSourceWritableRowRedirection(sparseRedirections);
            }
            case Hash: {
                final WritableRowRedirection rowRedirection =
                        WritableRowRedirectionLockFree.FACTORY.createRowRedirection(leftTable.intSize());

                try (final RowSet.Iterator indexKeyIt = indexTableRowSet.iterator()) {
                    for (int ii = 0; ii < rowSetCount; ++ii) {
                        final long indexRowKey = indexKeyIt.nextLong();
                        final long rightSide = groupPositionToRightSide.applyAsLong(ii);

                        if (rightSide == NO_RIGHT_ENTRY_VALUE) {
                            checkExactMatchForGroup(keySourcesFromLeftTable, indexTableRowSet, leftRowSets, ii);
                        } else {
                            final RowSet leftRowSetForKey = leftRowSets.get(indexRowKey);
                            leftRowSetForKey.forAllRowKeys((long ll) -> rowRedirection.put(ll, rightSide));
                        }
                    }
                }

                return rowRedirection;
            }
        }
        throw new IllegalStateException("Bad redirectionType: " + redirectionType);
    }

    /**
     * Check for an {@link NaturalJoinType#EXACTLY_ONE_MATCH} violation for the group at {@code groupPosition}, which
     * has no right-side match. The failing key is rendered from {@code keySourcesForErrorMessages}, which are columns
     * of the left table or of the data index table depending on how this state manager was constructed, so the group
     * must be mapped to a row key in the matching keyspace.
     */
    private void checkExactMatchForGroup(final boolean keySourcesFromLeftTable, final RowSet indexTableRowSet,
            final ColumnSource<RowSet> leftRowSets, final long groupPosition) {
        if (joinType != NaturalJoinType.EXACTLY_ONE_MATCH) {
            return;
        }
        final long indexRowKey = indexTableRowSet.get(groupPosition);
        final long errorRowKey = keySourcesFromLeftTable ? leftRowSets.get(indexRowKey).firstRowKey() : indexRowKey;
        checkExactMatch(errorRowKey, NO_RIGHT_ENTRY_VALUE);
    }

    /**
     * Throw the duplicate right key error for the first build position whose slot was marked as a duplicate.
     *
     * @param size the number of build positions
     * @param positionToRightSide maps a build position to its slot's right state
     * @param positionToErrorRowKey maps a build position to a row key in the keyspace of
     *        {@code keySourcesForErrorMessages} (the left table when built from the left input, the data index table
     *        when built from a left data index)
     */
    public void errorOnDuplicates(long size, LongUnaryOperator positionToRightSide,
            LongUnaryOperator positionToErrorRowKey) {
        for (int ii = 0; ii < size; ++ii) {
            final long rightSide = positionToRightSide.applyAsLong(ii);
            if (rightSide == DUPLICATE_RIGHT_VALUE) {
                throw new DuplicateRightKeyException("Natural Join found duplicate right key for "
                        + extractKeyStringFromSourceTable(positionToErrorRowKey.applyAsLong(ii)));
            }
        }
    }
}
