//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.LongChunk;
import io.deephaven.engine.exceptions.OutOfKeySpaceException;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.join.KeyIdHasherTypedBase;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import org.jetbrains.annotations.NotNull;

/**
 * This is our JoinStateManager for cross join when right is static and left is ticking.
 * <p>
 * Each distinct right key has an id from a {@link KeyIdHasherTypedBase}, which serves as the key's slot: the right rows
 * with that key are collected into {@link CrossJoinRightRowSets}, and each left row whose key is on the right is
 * redirected to the slot. Left rows only probe, so the table holds exactly the right keys.
 */
class LeftOnlyIncrementalChunkedCrossJoinStateManager
        extends CrossJoinShiftState
        implements CrossJoinStateManager {

    @FunctionalInterface
    interface StateTrackingCallback {

        /**
         * Invoke a callback that will allow external trackers to record changes to states in build or probe calls.
         *
         * @param stateSlot The state slot, or {@link RowSet#NULL_ROW_KEY} if the key is not on the right
         * @param rowKey The probed rowKey key
         */
        void invoke(long stateSlot, long rowKey);
    }

    @FunctionalInterface
    interface StateTrackingCallbackWithRightIndex {

        /**
         * Invoke a callback that will allow external trackers to record changes to states in build or probe calls.
         *
         * @param stateSlot The state slot, or {@link RowSet#NULL_ROW_KEY} if the key is not on the right
         * @param rowKey The probed rowKey key
         * @param rightRowSet The right RowSet
         */
        void invoke(long stateSlot, long rowKey, RowSet rightRowSet);
    }

    private static final TrackingRowSet EMPTY_ROWSET = RowSetFactory.empty().toTracking();

    private final KeyIdHasherTypedBase hasher;

    // the right rows of each key, by slot
    private final CrossJoinRightRowSets rightRowSets = new CrossJoinRightRowSets();

    // maintain a mapping from left rowKey to its slot
    private final WritableRowRedirection leftRowToSlot;
    private final ColumnSource<?>[] leftKeySources;
    private final QueryTable leftTable;

    LeftOnlyIncrementalChunkedCrossJoinStateManager(ColumnSource<?>[] tableKeySources,
            int tableSize,
            double maximumLoadFactor,
            QueryTable leftTable,
            int numRightBitsToReserve,
            boolean leftOuterJoin) {
        super(numRightBitsToReserve, leftOuterJoin);
        this.hasher = KeyIdHasherTypedBase.make(tableKeySources, tableSize, maximumLoadFactor);
        this.leftRowToSlot = WritableRowRedirection.FACTORY.createRowRedirection(tableSize);
        this.leftKeySources = tableKeySources;
        this.leftTable = leftTable;
    }

    @NotNull
    WritableRowSet buildLeftTicking(@NotNull final QueryTable leftTable,
            @NotNull final QueryTable rightTable,
            @NotNull final ColumnSource<?>[] rightKeys) {
        hasher.build(rightTable.getRowSet(), rightKeys,
                (rows, slots) -> rightRowSets.add(rows, slots, hasher.idCapacity()));
        rightRowSets.build(hasher.idCapacity());

        final int numBitsNeeded = CrossJoinShiftState.getMinBits(rightRowSets.maxGroupSize() - 1);
        if (numBitsNeeded > getNumShiftBits()) {
            setNumShiftBits(numBitsNeeded);
        }

        // We can now validate key-space after all of our right rows have been aggregated into groups, which determined
        // how many bits we need for the right keys.
        validateKeySpaceSize();

        final RowSetBuilderRandom resultRowSet = RowSetFactory.builderRandom();
        addLeft(leftTable.getRowSet(), (slot, rowKey, rightRowSet) -> {
            final long regionStart = rowKey << getNumShiftBits();
            if (rightRowSet.isNonempty()) {
                resultRowSet.addRange(regionStart, regionStart + rightRowSet.size() - 1);
            } else if (leftOuterJoin()) {
                resultRowSet.addKey(regionStart);
            }
        });

        return resultRowSet.build();
    }

    void removeLeft(final RowSet leftToRemove, final StateTrackingCallback trackingCallback) {
        final boolean usePrev = true;
        hasher.probe(leftToRemove, leftKeySources, usePrev, (rows, slots) -> {
            final LongChunk<OrderedRowKeys> rowKeys = rows.asRowKeyChunk();
            for (int ii = 0; ii < rowKeys.size(); ++ii) {
                final long leftKey = rowKeys.get(ii);
                leftRowToSlot.removeVoid(leftKey);
                trackingCallback.invoke(toStateSlot(slots.get(ii)), leftKey);
            }
        });
    }

    void addLeft(final RowSet leftToAdd, final StateTrackingCallbackWithRightIndex trackingCallback) {
        final boolean usePrev = false;
        hasher.probe(leftToAdd, leftKeySources, usePrev, (rows, slots) -> {
            final LongChunk<OrderedRowKeys> rowKeys = rows.asRowKeyChunk();
            for (int ii = 0; ii < rowKeys.size(); ++ii) {
                final long rowKey = rowKeys.get(ii);
                final long slot = toStateSlot(slots.get(ii));
                final RowSet rightRowSet = getRightRowSet(slot);
                if (slot != RowSet.NULL_ROW_KEY) {
                    leftRowToSlot.putVoid(rowKey, slot);
                }
                if (leftOuterJoin() || rightRowSet.isNonempty()) {
                    trackingCallback.invoke(slot, rowKey, rightRowSet);
                }
            }
        });
    }

    void applyLeftShift(final RowSet prevLeftRowSet, final RowSetShiftData shiftData) {
        leftRowToSlot.applyShift(prevLeftRowSet, shiftData);
    }

    void processLeftModifies(final TableUpdate upstream, final TableUpdateImpl downstream,
            final WritableRowSet resultRowSet) {
        if (upstream.modified().isEmpty()) {
            downstream.modified = RowSetFactory.empty();
            return;
        }
        final boolean usePrev = false;
        final RowSetBuilderSequential addBuilder = RowSetFactory.builderSequential();
        final RowSetBuilderSequential rmBuilder = RowSetFactory.builderSequential();
        final RowSetBuilderSequential modBuilder = RowSetFactory.builderSequential();
        final RowSetBuilderSequential rmResultBuilder = RowSetFactory.builderSequential();
        try (final RowSet.Iterator pit = upstream.getModifiedPreShift().iterator()) {
            hasher.probe(upstream.modified(), leftKeySources, usePrev, (rows, slots) -> {
                final LongChunk<OrderedRowKeys> rowKeys = rows.asRowKeyChunk();
                for (int ii = 0; ii < rowKeys.size(); ++ii) {
                    final long currKey = rowKeys.get(ii);
                    final long prevKey = pit.nextLong();
                    final long prevSlot = leftRowToSlot.getPrev(prevKey);
                    final long currSlot = toStateSlot(slots.get(ii));
                    final long regionStart = currKey << getNumShiftBits();
                    final long currentSize = getRightRowSet(currSlot).size();
                    if (prevSlot == currSlot) {
                        if (currentSize > 0) {
                            modBuilder.appendRange(regionStart, regionStart + currentSize - 1);
                        } else if (leftOuterJoin()) {
                            modBuilder.appendKey(regionStart);
                        }
                    } else {
                        final long prevSize = getRightRowSet(prevSlot).size();
                        final long prevRegionStart = prevKey << getNumShiftBits();
                        if (currSlot == RowSet.NULL_ROW_KEY) {
                            leftRowToSlot.removeVoid(currKey);
                        } else {
                            leftRowToSlot.putVoid(currKey, currSlot);
                        }

                        if (prevSize > 0) {
                            // note: removes are in old key space, but we have already shifted our result RowSet after
                            // processing upstream removals
                            rmBuilder.appendRange(prevRegionStart, prevRegionStart + prevSize - 1);
                            rmResultBuilder.appendRange(regionStart, regionStart + prevSize - 1);
                        } else if (leftOuterJoin()) {
                            rmBuilder.appendKey(prevRegionStart);
                            rmResultBuilder.appendKey(regionStart);
                        }
                        if (currentSize > 0) {
                            addBuilder.appendRange(regionStart, regionStart + currentSize - 1);
                        } else if (leftOuterJoin()) {
                            addBuilder.appendKey(regionStart);
                        }
                    }
                }
            });
        }
        try (final WritableRowSet added = addBuilder.build();
                final RowSet removed = rmBuilder.build();
                final RowSet postShiftRemoved = rmResultBuilder.build()) {
            downstream.removed().writableCast().insert(removed);
            downstream.added().writableCast().insert(added);
            // must remove before adding as removed.intersect(added) may be non-empty
            resultRowSet.remove(postShiftRemoved);
            resultRowSet.subsume(added);
        }
        downstream.modified = modBuilder.build();
    }

    void startTrackingPrevValues() {
        leftRowToSlot.startTrackingPrevValues();
    }

    private static long toStateSlot(final int id) {
        return id == KeyIdHasherTypedBase.NULL_ID ? RowSet.NULL_ROW_KEY : id;
    }

    /**
     * @param slot a slot, or {@link RowSet#NULL_ROW_KEY}
     * @return the right rows of the slot, or an empty row set for {@link RowSet#NULL_ROW_KEY}
     */
    public TrackingRowSet getRightRowSet(long slot) {
        if (slot == RowSet.NULL_ROW_KEY) {
            return EMPTY_ROWSET;
        }
        final TrackingRowSet rightRowSet = rightRowSets.get(slot);
        return rightRowSet == null ? EMPTY_ROWSET : rightRowSet;
    }

    // the left side never has a slot without right rows, because the slots are only created by the right side
    public long getRightSize(long slot) {
        Assert.neq(slot, "slot", RowSet.NULL_ROW_KEY);
        final RowSet rightRowSet = rightRowSets.get(slot);
        Assert.neqNull(rightRowSet, "rightRowSet");
        return rightRowSet.size();
    }

    @Override
    public TrackingRowSet getRightRowSetFromLeftRow(long leftRow) {
        return getRightRowSet(leftRowToSlot.get(leftRow));
    }

    @Override
    public TrackingRowSet getRightRowSetFromPrevLeftRow(long leftRow) {
        return getRightRowSet(leftRowToSlot.getPrev(leftRow));
    }

    public void validateKeySpaceSize() {
        final long leftLastKey = leftTable.getRowSet().lastRowKey();
        final long rightLastKey = rightRowSets.maxGroupSize() - 1;
        final int minLeftBits = CrossJoinShiftState.getMinBits(leftLastKey);
        final int minRightBits = CrossJoinShiftState.getMinBits(rightLastKey);
        final int numShiftBits = getNumShiftBits();
        if (minLeftBits + numShiftBits > 63) {
            throw new OutOfKeySpaceException("join out of rowSet space (left reqBits + right reservedBits > 63): "
                    + "(left table: {size: " + leftTable.getRowSet().size() + " maxRowKey: " + leftLastKey
                    + " reqBits: " + minLeftBits + "}) X "
                    + "(right table: {maxRowKeyUsed: " + rightLastKey + " reqBits: " + minRightBits
                    + " reservedBits: " + numShiftBits + "})"
                    + " exceeds Long.MAX_VALUE. Consider flattening left table or reserving fewer right bits if possible.");
        }
    }
}
