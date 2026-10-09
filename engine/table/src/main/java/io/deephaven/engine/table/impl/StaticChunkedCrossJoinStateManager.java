//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.LongChunk;
import io.deephaven.engine.exceptions.OutOfKeySpaceException;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.join.KeyIdHasherTypedBase;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import org.jetbrains.annotations.NotNull;

/**
 * The cross join state manager when both inputs are static.
 * <p>
 * Each distinct key has an id from a {@link KeyIdHasherTypedBase}; the right rows with that key are collected into
 * {@link CrossJoinRightRowSets}, and each left row is redirected to the id of its key.
 */
class StaticChunkedCrossJoinStateManager
        extends CrossJoinShiftState
        implements CrossJoinStateManager {

    static final TrackingRowSet EMPTY_ROWSET = RowSetFactory.empty().toTracking();

    private final KeyIdHasherTypedBase hasher;

    // the right rows of each key, by id
    private final CrossJoinRightRowSets rightRowSets = new CrossJoinRightRowSets();

    // the id of each left row's key
    private final WritableRowRedirection leftRowSetToSlot;

    StaticChunkedCrossJoinStateManager(ColumnSource<?>[] tableKeySources, int tableSize, JoinControl control,
            QueryTable leftTable, boolean leftOuterJoin) {
        // on a static build, we can use minimum number of right bits since we compute the largest right RowSet on
        // construction and the left doesn't tick; so we will do RowSet related work exactly once
        super(1, leftOuterJoin);
        hasher = KeyIdHasherTypedBase.make(tableKeySources, tableSize, control.getMaximumLoadFactor());
        leftRowSetToSlot = JoinRowRedirection.makeRowRedirection(control, leftTable);
    }

    @NotNull
    WritableRowSet buildFromRight(@NotNull final QueryTable leftTable,
            @NotNull final ColumnSource<?>[] leftKeys,
            @NotNull final QueryTable rightTable,
            @NotNull final ColumnSource<?>[] rightKeys) {
        hasher.build(rightTable.getRowSet(), rightKeys,
                (rows, ids) -> rightRowSets.add(rows, ids, hasher.idCapacity()));
        rightRowSets.build(hasher.idCapacity());

        // We can now validate key-space after all of our right rows have been aggregated into groups, which determined
        // how many bits we need for the right keyspace.
        updateBitsNeeded(rightRowSets.maxGroupSize());
        validateKeySpaceSize(leftTable);

        final RowSetBuilderSequential resultRowSet = RowSetFactory.builderSequential();
        hasher.probe(leftTable.getRowSet(), leftKeys, false, (rows, ids) -> {
            final LongChunk<OrderedRowKeys> rowKeys = rows.asRowKeyChunk();
            for (int ii = 0; ii < rowKeys.size(); ++ii) {
                final long rowKey = rowKeys.get(ii);
                final int id = ids.get(ii);
                final long regionStart = rowKey << getNumShiftBits();
                if (id != KeyIdHasherTypedBase.NULL_ID) {
                    final RowSet rightRowSet = getRightRowSetForSlot(rightRowSets, id);
                    Assert.assertion(rightRowSet.isNonempty(), "rightRowSet.nonEmpty()");
                    leftRowSetToSlot.put(rowKey, id);
                    resultRowSet.appendRange(regionStart, regionStart + rightRowSet.size() - 1);
                } else if (leftOuterJoin()) {
                    resultRowSet.appendKey(regionStart);
                }
            }
        });

        return resultRowSet.build();
    }

    @NotNull
    WritableRowSet buildFromLeft(@NotNull final QueryTable leftTable,
            @NotNull final ColumnSource<?>[] leftKeys,
            @NotNull final QueryTable rightTable,
            @NotNull final ColumnSource<?>[] rightKeys) {
        hasher.build(leftTable.getRowSet(), leftKeys, (rows, ids) -> {
            final LongChunk<OrderedRowKeys> rowKeys = rows.asRowKeyChunk();
            for (int ii = 0; ii < rowKeys.size(); ++ii) {
                leftRowSetToSlot.put(rowKeys.get(ii), ids.get(ii));
            }
        });

        // only the keys that are on the left need right rows
        hasher.probe(rightTable.getRowSet(), rightKeys, false,
                (rows, ids) -> rightRowSets.add(rows, ids, hasher.idCapacity()));
        rightRowSets.build(hasher.idCapacity());

        // We can now validate key-space after all of our right rows have been aggregated into groups, which determined
        // how many bits we need for the right keyspace.
        updateBitsNeeded(rightRowSets.maxGroupSize());
        validateKeySpaceSize(leftTable);

        final RowSetBuilderSequential resultRowSet = RowSetFactory.builderSequential();
        leftTable.getRowSet().forAllRowKeys(ii -> {
            final long regionStart = ii << getNumShiftBits();
            final RowSet rightRowSet = getRightRowSetFromLeftRow(ii);
            if (rightRowSet.isNonempty()) {
                resultRowSet.appendRange(regionStart, regionStart + rightRowSet.size() - 1);
            } else if (leftOuterJoin()) {
                resultRowSet.appendKey(regionStart);
            }
        });

        return resultRowSet.build();
    }

    private void updateBitsNeeded(long size) {
        final int numBitsNeeded = CrossJoinShiftState.getMinBits(size - 1);
        if (numBitsNeeded > getNumShiftBits()) {
            setNumShiftBits(numBitsNeeded);
        }
    }

    ResultOnlyCrossJoinStateManager getResultOnlyStateManager() {
        return new ResultOnlyCrossJoinStateManager(rightRowSets, leftRowSetToSlot, getNumShiftBits(),
                leftOuterJoin());
    }

    /**
     * For the result we do not need to maintain the hash table, we only need to have the densely packed set of right
     * indices and the redirection from the left table to the corresponding RowSet. By returning this simple state
     * manager instead of preserving the full StaticChunkedCrossJoinStateManager we can drop the intermediate table.
     */
    static class ResultOnlyCrossJoinStateManager extends CrossJoinShiftState implements CrossJoinStateManager {
        private final CrossJoinRightRowSets rightRowSets;
        private final RowRedirection leftRowSetToSlot;

        public ResultOnlyCrossJoinStateManager(
                CrossJoinRightRowSets rightRowSets,
                RowRedirection leftRowSetToSlot,
                int numBits,
                boolean allowRightSideNulls) {
            super(numBits, allowRightSideNulls);
            this.rightRowSets = rightRowSets;
            this.leftRowSetToSlot = leftRowSetToSlot;
        }

        @Override
        public TrackingRowSet getRightRowSetFromLeftRow(long leftRowSlot) {
            return StaticChunkedCrossJoinStateManager.getRightRowSetFromLeftRowKey(leftRowSetToSlot, rightRowSets,
                    leftRowSlot);
        }

        @Override
        public TrackingRowSet getRightRowSetFromPrevLeftRow(long leftRowKey) {
            return getRightRowSetFromLeftRow(leftRowKey);
        }
    }

    @NotNull
    private static TrackingRowSet getRightRowSetForSlot(CrossJoinRightRowSets rightRowSets, long rowSetSlot) {
        final TrackingRowSet retVal = rightRowSets.get(rowSetSlot);
        if (retVal != null) {
            return retVal;
        }
        return EMPTY_ROWSET;
    }

    @Override
    public TrackingRowSet getRightRowSetFromLeftRow(long leftRowKey) {
        return getRightRowSetFromLeftRowKey(leftRowSetToSlot, rightRowSets, leftRowKey);
    }

    @NotNull
    private static TrackingRowSet getRightRowSetFromLeftRowKey(
            RowRedirection leftRowSetToSlot, CrossJoinRightRowSets rightRowSets, long leftRowKey) {
        long slot = leftRowSetToSlot.get(leftRowKey);
        if (slot == RowSet.NULL_ROW_KEY) {
            return EMPTY_ROWSET;
        }
        return getRightRowSetForSlot(rightRowSets, slot);
    }

    @Override
    public TrackingRowSet getRightRowSetFromPrevLeftRow(long leftRowKey) {
        // static has no prev
        return getRightRowSetFromLeftRow(leftRowKey);
    }

    private void validateKeySpaceSize(final QueryTable leftTable) {
        final long leftLastKey = leftTable.getRowSet().lastRowKey();
        final long rightLastKey = rightRowSets.maxGroupSize() - 1;
        final int minLeftBits = CrossJoinShiftState.getMinBits(leftLastKey);
        final int minRightBits = getNumShiftBits();
        if (minLeftBits + minRightBits > 63) {
            throw new OutOfKeySpaceException("join out of rowSet space (left reqBits + right reqBits > 63): "
                    + "(left table: {size: " + leftTable.getRowSet().size() + " maxRowKey: " + leftLastKey
                    + " reqBits: " + minLeftBits + "}) X "
                    + "(right table: {maxRowKey: " + rightLastKey + " reqBits: " + minRightBits + "})"
                    + " exceeds Long.MAX_VALUE. Consider flattening left table if possible.");
        }
    }
}
