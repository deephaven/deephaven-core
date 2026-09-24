//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.naturaljoin;

import io.deephaven.api.NaturalJoinType;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.JoinControl;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.StaticNaturalJoinStateManager;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.table.impl.util.ContiguousWritableRowRedirection;
import io.deephaven.engine.table.impl.util.LongColumnSourceWritableRowRedirection;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.engine.table.impl.util.WritableRowRedirectionLockFree;

import java.util.function.LongUnaryOperator;

public abstract class StaticHashedNaturalJoinStateManager extends StaticNaturalJoinStateManager {

    protected StaticHashedNaturalJoinStateManager(
            ColumnSource<?>[] keySourcesForErrorMessages,
            NaturalJoinType joinType,
            boolean addOnly) {
        super(keySourcesForErrorMessages, joinType, addOnly);
    }

    public abstract void buildFromLeftSide(
            final Table leftTable,
            final ColumnSource<?>[] leftSources,
            final IntegerArraySource leftHashSlots);

    public abstract void buildFromRightSide(
            final Table rightTable,
            final ColumnSource<?>[] rightSources);

    public abstract void decorateLeftSide(
            final RowSet leftRowSet,
            final ColumnSource<?>[] leftSources,
            final LongArraySource leftRedirections);

    /**
     * Probe the rows of a left data index table, storing one redirection per group. A duplicate right key error names
     * the key of the offending group's first left row, since {@code keySourcesForErrorMessages} are columns of the left
     * table rather than of the data index table.
     *
     * @param indexTableRowSet the data index table's row set
     * @param indexSources the data index table's key columns
     * @param indexRowSets the data index table's row set column, mapping each group to its left rows
     * @param leftRedirections receives the right row key (or {@link RowSet#NULL_ROW_KEY}) for each group, by position
     */
    public abstract void decorateLeftSideIndexed(
            final RowSet indexTableRowSet,
            final ColumnSource<?>[] indexSources,
            final ColumnSource<RowSet> indexRowSets,
            final LongArraySource leftRedirections);

    public abstract void decorateWithRightSide(
            final Table rightTable,
            final ColumnSource<?>[] rightSources);

    public abstract WritableRowRedirection buildRowRedirectionFromHashSlot(QueryTable leftTable,
            IntegerArraySource leftHashSlots, JoinControl.RedirectionType redirectionType);

    public abstract WritableRowRedirection buildRowRedirectionFromRedirections(QueryTable leftTable,
            LongArraySource leftRedirections, JoinControl.RedirectionType redirectionType);

    public abstract WritableRowRedirection buildIndexedRowRedirectionFromRedirections(QueryTable leftTable,
            RowSet indexTableRowSet, LongArraySource leftRedirections,
            ColumnSource<RowSet> indexRowSets, JoinControl.RedirectionType redirectionType);

    public abstract WritableRowRedirection buildIndexedRowRedirectionFromHashSlots(QueryTable leftTable,
            RowSet indexTableRowSet, IntegerArraySource leftHashSlots,
            ColumnSource<RowSet> indexRowSets, JoinControl.RedirectionType redirectionType);

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
                throw new IllegalStateException("Natural Join found duplicate right key for "
                        + extractKeyStringFromSourceTable(positionToErrorRowKey.applyAsLong(ii)));
            }
        }
    }

    /**
     * Throw the duplicate right key error after a build from a left data index; the error key sources are columns of
     * the data index table.
     *
     * @param leftHashSlots the hash slot of each data index table row, by position
     * @param indexTableRowSet the data index table's row set
     */
    public abstract void errorOnDuplicatesIndexed(IntegerArraySource leftHashSlots, RowSet indexTableRowSet);

    /**
     * Throw the duplicate right key error after a build from the left table; the error key sources are columns of the
     * left table.
     *
     * @param leftHashSlots the hash slot of each left row, by position
     * @param size the number of left rows
     * @param rowSet the left table's row set
     */
    public abstract void errorOnDuplicatesSingle(IntegerArraySource leftHashSlots, long size, RowSet rowSet);
}
