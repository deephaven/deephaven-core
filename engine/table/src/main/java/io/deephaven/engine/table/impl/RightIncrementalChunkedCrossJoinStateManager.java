//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.exceptions.OutOfKeySpaceException;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.join.IncrementalKeyIdHasherTypedBase;
import io.deephaven.engine.table.impl.join.KeyIdHasher;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.TestUseOnly;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;

import static io.deephaven.engine.table.impl.JoinControl.CHUNK_SIZE;

/**
 * This is our JoinStateManager for cross join when right is ticking (left may be static or ticking).
 * <p>
 * Each distinct key has an id from an {@link IncrementalKeyIdHasherTypedBase}, which serves as the key's slot: the left
 * and right row sets, the modified slot tracker cookie, and the row redirections all refer to the id. When both sides
 * tick, a key whose left and right rows are all gone is removed at the end of the update, and its id is reused by a
 * later key.
 */
class RightIncrementalChunkedCrossJoinStateManager
        extends CrossJoinShiftState
        implements CrossJoinStateManager {

    @FunctionalInterface
    interface StateTrackingCallback {

        /**
         * Invoke a callback that will allow external trackers to record changes to states in build or probe calls.
         *
         * @param cookie The last known cookie for state slot
         * @param stateSlot The state slot
         * @param rowKey The probed row key
         * @param prevRowKey The probed prev row key (applicable only when prevRowKey provided to build/probe otherwise
         *        RowSet.NULL_ROW_KEY)
         * @return The new cookie for the state
         */
        long invoke(long cookie, long stateSlot, long rowKey, long prevRowKey);
    }

    public static final long LEFT_MAPPING_MISSING = RowSequence.NULL_ROW_KEY;
    private static final long EMPTY_RIGHT_SLOT = RowSequence.NULL_ROW_KEY;

    private final IncrementalKeyIdHasherTypedBase hasher;

    // maintain a mapping from left rowKey to its slot
    private final WritableRowRedirection leftRowToSlot;
    // maintain a mapping from right rowKey to its slot
    private final WritableRowRedirection rightRowToSlot;
    private final ColumnSource<?>[] leftKeySources;
    private final ColumnSource<?>[] rightKeySources;

    // the row sets and modified slot tracker cookie of each slot
    private final ObjectArraySource<TrackingWritableRowSet> leftRowSetSource =
            new ObjectArraySource<>(TrackingWritableRowSet.class);
    private final ObjectArraySource<TrackingWritableRowSet> rightRowSetSource =
            new ObjectArraySource<>(TrackingWritableRowSet.class);
    private final LongArraySource modifiedTrackerCookieSource = new LongArraySource();
    private int slotCapacity = 0;

    // the right row sets of slots released by the last update, which its readers may still see as previous values
    private final List<TrackingWritableRowSet> releasedRightRowSets = new ArrayList<>();

    private final boolean isLeftTicking;
    private final QueryTable leftTable;
    private long maxRightGroupSize = 0;

    RightIncrementalChunkedCrossJoinStateManager(ColumnSource<?>[] tableKeySources,
            int tableSize,
            double maximumLoadFactor,
            ColumnSource<?>[] rightKeySources,
            QueryTable leftTable,
            int initialNumRightBits,
            boolean leftOuterJoin) {
        super(initialNumRightBits, leftOuterJoin);
        this.hasher = IncrementalKeyIdHasherTypedBase.make(tableKeySources, tableSize, maximumLoadFactor);
        this.leftRowToSlot = WritableRowRedirection.FACTORY.createRowRedirection(tableSize);
        this.rightRowToSlot = WritableRowRedirection.FACTORY.createRowRedirection(tableSize);
        this.leftKeySources = tableKeySources;
        this.rightKeySources = rightKeySources;
        this.isLeftTicking = leftTable.isRefreshing();
        this.leftTable = leftTable;
    }

    @NotNull
    WritableRowSet build(@NotNull final QueryTable leftTable,
            @NotNull final QueryTable rightTable) {
        // This state manager assumes right side is ticking.
        Assert.eqTrue(rightTable.isRefreshing(), "rightTable.isRefreshing()");
        // nothing is waiting on the initial build, so it can rehash all at once
        final boolean initialBuild = true;
        buildWithCallback(initialBuild, leftTable.getRowSet(), leftKeySources, null,
                (cookie, slot, rowKey, prevRowKey) -> addToRowSet(true, slot, rowKey));

        if (isLeftTicking) {
            buildWithCallback(initialBuild, rightTable.getRowSet(), rightKeySources, null,
                    (cookie, slot, rowKey, prevRowKey) -> addToRowSet(false, slot, rowKey));
        } else {
            // we don't actually need to create groups that don't (and will never) exist on the left; thus can
            // probe-only
            probeWithCallback(rightTable.getRowSet(), rightKeySources, false, null,
                    (cookie, slot, rowKey, prevRowKey) -> addToRowSet(false, slot, rowKey));
        }

        // We can now validate key-space after all of our right rows have been aggregated into groups, which determined
        // how many bits we need for the right keyspace.
        validateKeySpaceSize();

        final RowSetBuilderSequential resultRowSet = RowSetFactory.builderSequential();
        leftTable.getRowSet().forAllRowKeys(rowKey -> {
            final long regionStart = rowKey << getNumShiftBits();
            final RowSet rightRowSet = getRightRowSetFromLeftRow(rowKey);
            if (rightRowSet.isNonempty()) {
                resultRowSet.appendRange(regionStart, regionStart + rightRowSet.size() - 1);
            } else if (leftOuterJoin()) {
                resultRowSet.appendKey(regionStart);
            }
        });

        return resultRowSet.build();
    }

    void rightRemove(final RowSet removed, final CrossJoinModifiedSlotTracker tracker) {
        if (removed.isEmpty()) {
            return;
        }
        try (final WritableLongChunk<RowKeys> rightToRemove =
                WritableLongChunk.makeWritableChunk((int) Math.min(CHUNK_SIZE, removed.size()))) {
            rightToRemove.setSize(0);
            final boolean usePrev = true;
            probeWithCallback(removed, rightKeySources, usePrev, null, (cookie, slot, rowKey, prevRowKey) -> {
                addRightRemove(rightToRemove, rowKey);
                return tracker.appendChunkRemove(cookie, slot, rowKey);
            });
            flushRightRemove(rightToRemove);
        }
    }

    private void addRightRemove(WritableLongChunk<RowKeys> rightToRemove, long rowKey) {
        rightToRemove.add(rowKey);
        if (rightToRemove.size() == rightToRemove.capacity()) {
            flushRightRemove(rightToRemove);
        }
    }

    private void flushRightRemove(WritableLongChunk<RowKeys> rightToRemove) {
        rightRowToSlot.removeAllUnordered(rightToRemove);
        rightToRemove.setSize(0);
    }

    void shiftRightRowSetToSlot(final RowSet filterRowSet, final RowSetShiftData shifted) {
        rightRowToSlot.applyShift(filterRowSet, shifted);
    }

    void rightShift(final RowSet filterRowSet, final RowSetShiftData shifted,
            final CrossJoinModifiedSlotTracker tracker) {
        shifted.forAllInRowSet(filterRowSet, (ii, delta) -> {
            final long slot = rightRowToSlot.get(ii);
            if (slot == RowSequence.NULL_ROW_KEY) {
                // right-ticking w/static-left does not maintain group states that will never be used
                return;
            }

            final long cookie = modifiedTrackerCookieSource.getUnsafe(slot);
            final long newCookie = tracker.needsRightShift(cookie, slot);
            if (newCookie != cookie) {
                modifiedTrackerCookieSource.set(slot, newCookie);
            }
        });
    }

    void rightAdd(final RowSet added, final CrossJoinModifiedSlotTracker tracker) {
        if (added.isEmpty()) {
            return;
        }

        final StateTrackingCallback addKeyCallback =
                (cookie, slot, rowKey, prevRowKey) -> tracker.appendChunkAdd(cookie, slot, rowKey);

        if (!isLeftTicking) {
            // when left is static we have no business creating new slots
            final boolean usePrev = false;
            probeWithCallback(added, rightKeySources, usePrev, null, addKeyCallback);
        } else {
            buildWithCallback(false, added, rightKeySources, null, addKeyCallback);
        }
    }

    void rightModified(final TableUpdate upstream, final boolean keyColumnsChanged,
            final CrossJoinModifiedSlotTracker tracker) {
        if (upstream.modified().isEmpty()) {
            return;
        }

        if (!keyColumnsChanged) {
            final boolean usePrev = false;
            probeWithCallback(upstream.modified(), rightKeySources, usePrev, null,
                    (cookie, slot, rowKey, prevRowKey) -> tracker.appendChunkModify(cookie, slot, rowKey));
            return;
        }

        try (final WritableLongChunk<RowKeys> rightRemoved = WritableLongChunk.makeWritableChunk(CHUNK_SIZE)) {
            rightRemoved.setSize(0);

            final StateTrackingCallback callback = (cookie, postSlot, rowKey, prevRowKey) -> {
                // TODO: ugly virtual call on the RowRedirection
                final long preSlot = rightRowToSlot.get(prevRowKey);

                if (preSlot != postSlot) {
                    if (preSlot != EMPTY_RIGHT_SLOT) {
                        final long oldCookie = modifiedTrackerCookieSource.getUnsafe(preSlot);
                        final long newCookie = tracker.appendChunkRemove(oldCookie, preSlot, prevRowKey);
                        if (oldCookie != newCookie) {
                            modifiedTrackerCookieSource.set(preSlot, newCookie);
                        }
                        addRightRemove(rightRemoved, prevRowKey);
                    }
                    if (postSlot != EMPTY_RIGHT_SLOT) {
                        cookie = tracker.appendChunkAdd(cookie, postSlot, rowKey);
                    }
                } else if (preSlot != EMPTY_RIGHT_SLOT) {
                    // note we must mark postShift rowKey as the modification
                    cookie = tracker.appendChunkModify(cookie, postSlot, rowKey);
                }
                return cookie;
            };

            if (isLeftTicking) {
                buildWithCallback(false, upstream.modified(), rightKeySources, upstream.getModifiedPreShift(),
                        callback);
            } else {
                final boolean usePrevOnPost = false;
                probeWithCallback(upstream.modified(), rightKeySources, usePrevOnPost,
                        upstream.getModifiedPreShift(), callback);
            }
            flushRightRemove(rightRemoved);
        }
    }

    void leftRemoved(final RowSet removed, final CrossJoinModifiedSlotTracker tracker) {
        if (removed.isNonempty()) {
            final boolean usePrev = true;
            probeWithCallback(removed, leftKeySources, usePrev, null, (cookie, slot, rowKey, prevRowKey) -> {
                leftRowToSlot.removeVoid(rowKey);
                return tracker.addToBuilder(cookie, slot, rowKey);
            });
        }
        tracker.flushLeftRemoves();
    }

    void leftAdded(final RowSet added, final CrossJoinModifiedSlotTracker tracker) {
        if (added.isNonempty()) {
            buildWithCallback(false, added, leftKeySources, null,
                    (cookie, slot, rowKey, prevRowKey) -> tracker.addToBuilder(cookie, slot, rowKey));
        }
        tracker.flushLeftAdds();
    }

    void leftModified(final TableUpdate upstream, final boolean keyColumnsChanged,
            final CrossJoinModifiedSlotTracker tracker) {
        if (upstream.modified().isEmpty()) {
            tracker.flushLeftModifies();
            return;
        }

        if (!keyColumnsChanged) {
            final boolean usePrev = false;
            probeWithCallback(upstream.modified(), leftKeySources, usePrev, null,
                    (cookie, slot, rowKey, prevRowKey) -> tracker.appendChunkModify(cookie, slot, rowKey));
            tracker.flushLeftModifies();
            return;
        }

        // note: at this point left shifts have not yet been applied to our internal data structures
        final StateTrackingCallback callback = (cookie, postSlot, rowKey, prevRowKey) -> {
            Assert.neq(postSlot, "postSlot", EMPTY_RIGHT_SLOT);
            final long preSlot = leftRowToSlot.get(prevRowKey);
            Assert.neq(preSlot, "preSlot", EMPTY_RIGHT_SLOT);

            if (preSlot != postSlot) {
                // unlike rightModified, leftRowToSlot is not yet shifted; so we operate in terms of pre-shift
                // value.
                final long oldCookie = modifiedTrackerCookieSource.getUnsafe(preSlot);
                final long newCookie = tracker.addToBuilder(oldCookie, preSlot, prevRowKey);
                if (oldCookie != newCookie) {
                    modifiedTrackerCookieSource.set(preSlot, newCookie);
                }
                cookie = tracker.appendChunkAdd(cookie, postSlot, rowKey);
            } else {
                // note we must mark post shift rowKey as the modification
                cookie = tracker.appendChunkModify(cookie, postSlot, rowKey);
            }
            return cookie;
        };

        buildWithCallback(false, upstream.modified(), leftKeySources, upstream.getModifiedPreShift(), callback);
        tracker.flushLeftModifies();
    }

    void leftShift(final RowSet filterRowSet, final RowSetShiftData shifted,
            final CrossJoinModifiedSlotTracker tracker) {
        shifted.forAllInRowSet(filterRowSet, (ii, delta) -> {
            final long slot = leftRowToSlot.get(ii);
            if (slot == RowSequence.NULL_ROW_KEY) {
                // This might happen if a RowSet is moving from one slot to another; we shift after removes but before
                // the adds. We don't need to shift the slot that was related to this RowSet.
                return;
            }

            final long cookie = modifiedTrackerCookieSource.getUnsafe(slot);
            final long newCookie = tracker.needsLeftShift(cookie, slot);
            if (newCookie != cookie) {
                modifiedTrackerCookieSource.set(slot, newCookie);
            }
        });
        leftRowToSlot.applyShift(filterRowSet, shifted);
    }

    /**
     * Remove the keys of the slots that the tracker visited this update and that no longer have any left or right rows.
     * Call this once the downstream update is complete, before the tracker is cleared; nothing refers to a released
     * slot afterwards except the previous value of its right row set, which this update's readers may still need.
     *
     * @param tracker the tracker for this update
     */
    void releaseEmptySlots(final CrossJoinModifiedSlotTracker tracker) {
        // the right row sets released by an earlier update are no longer anyone's previous value
        releasedRightRowSets.forEach(RowSet::close);
        releasedRightRowSets.clear();
        try (final WritableIntChunk<Values> slotsToRemove = WritableIntChunk.makeWritableChunk(CHUNK_SIZE)) {
            slotsToRemove.setSize(0);
            tracker.forAllSlotStates(slotState -> {
                if (slotState.leftRowSet.isNonempty() || slotState.rightRowSet.isNonempty()) {
                    return;
                }
                final int slot = Math.toIntExact(slotState.slotLocation);
                releaseSlotState(slot);
                slotsToRemove.add(slot);
                if (slotsToRemove.size() == slotsToRemove.capacity()) {
                    hasher.remove(slotsToRemove);
                    slotsToRemove.setSize(0);
                }
            });
            hasher.remove(slotsToRemove);
        }
    }

    /**
     * Close or set aside the row sets of a released slot, and forget its tracker cookie.
     */
    private void releaseSlotState(final int slot) {
        final TrackingWritableRowSet leftRowSet = leftRowSetSource.getAndSetUnsafe(slot, null);
        if (leftRowSet != null) {
            leftRowSet.close();
        }
        final TrackingWritableRowSet rightRowSet = rightRowSetSource.getUnsafe(slot);
        rightRowSetSource.set(slot, null);
        if (rightRowSet != null) {
            releasedRightRowSets.add(rightRowSet);
        }
        modifiedTrackerCookieSource.set(slot, CrossJoinModifiedSlotTracker.NULL_COOKIE);
    }

    /**
     * Build the keys of {@code rows}, invoking {@code trackingCallback} for each row with its key's slot.
     *
     * @param initialBuild whether this is the initial build, which grows the table by full rehashes
     * @param prevRows if not null, rows parallel to {@code rows} whose keys are passed as the callback's prevRowKey
     */
    private void buildWithCallback(
            final boolean initialBuild,
            final RowSet rows,
            final ColumnSource<?>[] sources,
            @Nullable final RowSet prevRows,
            final StateTrackingCallback trackingCallback) {
        if (rows.isEmpty()) {
            return;
        }
        try (final PrevRowKeys prevRowKeys = new PrevRowKeys(rows, prevRows)) {
            final KeyIdHasher.IdChunkConsumer consumer = (chunkRows, slots) -> {
                ensureSlotCapacity();
                final LongChunk<OrderedRowKeys> rowKeys = chunkRows.asRowKeyChunk();
                final LongChunk<OrderedRowKeys> prevKeys = prevRowKeys.next(chunkRows);
                for (int ii = 0; ii < rowKeys.size(); ++ii) {
                    final int slot = slots.get(ii);
                    ensureSlotExists(slot);
                    invokeTrackingCallback(trackingCallback, slot, rowKeys.get(ii),
                            prevKeys == null ? RowSequence.NULL_ROW_KEY : prevKeys.get(ii));
                }
            };
            if (initialBuild) {
                hasher.buildWithFullRehash(rows, sources, consumer);
            } else {
                hasher.build(rows, sources, consumer);
            }
        }
    }

    /**
     * Probe the keys of {@code rows}, invoking {@code trackingCallback} for each row whose key has a slot. When
     * {@code prevRows} is provided, the callback is also invoked with {@link #EMPTY_RIGHT_SLOT} for each row whose key
     * has none.
     *
     * @param prevRows if not null, rows parallel to {@code rows} whose keys are passed as the callback's prevRowKey
     */
    private void probeWithCallback(
            final RowSet rows,
            final ColumnSource<?>[] sources,
            final boolean usePrev,
            @Nullable final RowSet prevRows,
            final StateTrackingCallback trackingCallback) {
        if (rows.isEmpty()) {
            return;
        }
        try (final PrevRowKeys prevRowKeys = new PrevRowKeys(rows, prevRows)) {
            hasher.probe(rows, sources, usePrev, (chunkRows, slots) -> {
                final LongChunk<OrderedRowKeys> rowKeys = chunkRows.asRowKeyChunk();
                final LongChunk<OrderedRowKeys> prevKeys = prevRowKeys.next(chunkRows);
                for (int ii = 0; ii < rowKeys.size(); ++ii) {
                    final int slot = slots.get(ii);
                    if (slot != KeyIdHasher.NULL_ID) {
                        invokeTrackingCallback(trackingCallback, slot, rowKeys.get(ii),
                                prevKeys == null ? RowSequence.NULL_ROW_KEY : prevKeys.get(ii));
                    } else if (prevKeys != null) {
                        trackingCallback.invoke(CrossJoinModifiedSlotTracker.NULL_COOKIE, EMPTY_RIGHT_SLOT,
                                rowKeys.get(ii), prevKeys.get(ii));
                    }
                }
            });
        }
    }

    private void invokeTrackingCallback(final StateTrackingCallback trackingCallback, final int slot,
            final long rowKey, final long prevRowKey) {
        final long oldCookie = modifiedTrackerCookieSource.getUnsafe(slot);
        final long newCookie = trackingCallback.invoke(oldCookie, slot, rowKey, prevRowKey);
        if (oldCookie != newCookie) {
            modifiedTrackerCookieSource.set(slot, newCookie);
        }
    }

    /**
     * Supplies the chunks of a row set parallel to the chunks of another, for the prevRowKey of a tracking callback.
     */
    private static final class PrevRowKeys implements SafeCloseable {
        private final RowSequence.Iterator prevIterator;

        private PrevRowKeys(final RowSet rows, @Nullable final RowSet prevRows) {
            if (prevRows == null) {
                prevIterator = null;
            } else {
                Assert.eq(prevRows.size(), "prevRows.size()", rows.size(), "rows.size()");
                prevIterator = prevRows.getRowSequenceIterator();
            }
        }

        /**
         * @return the prev row keys parallel to {@code chunkRows}, the next chunk of rows, or null if there are none
         */
        private LongChunk<OrderedRowKeys> next(final RowSequence chunkRows) {
            return prevIterator == null ? null
                    : prevIterator.getNextRowSequenceWithLength(chunkRows.size()).asRowKeyChunk();
        }

        @Override
        public void close() {
            if (prevIterator != null) {
                prevIterator.close();
            }
        }
    }

    private void ensureSlotCapacity() {
        final int capacity = hasher.idCapacity();
        if (capacity > slotCapacity) {
            leftRowSetSource.ensureCapacity(capacity);
            rightRowSetSource.ensureCapacity(capacity);
            modifiedTrackerCookieSource.ensureCapacity(capacity);
            slotCapacity = capacity;
        }
    }

    private void ensureSlotExists(final long slot) {
        final RowSet rowSet = rightRowSetSource.getUnsafe(slot);
        if (rowSet == null) {
            rightRowSetSource.set(slot, RowSetFactory.empty().toTracking());
        }
    }

    private long addToRowSet(final boolean isLeft, final long slot, final long keyToAdd) {
        final ObjectArraySource<TrackingWritableRowSet> source = isLeft ? leftRowSetSource : rightRowSetSource;

        final long size;
        final WritableRowSet rowSet = source.get(slot);
        if (rowSet == null) {
            source.set(slot, RowSetFactory.fromKeys(keyToAdd).toTracking());
            size = 1;
        } else {
            rowSet.insert(keyToAdd);
            size = rowSet.size();
        }

        if (isLeft) {
            leftRowToSlot.put(keyToAdd, slot);
        } else {
            rightRowToSlot.put(keyToAdd, slot);

            // only right side insertions can cause shifts
            final int numBitsNeeded = CrossJoinShiftState.getMinBits(size - 1);
            if (numBitsNeeded > getNumShiftBits()) {
                setNumShiftBits(numBitsNeeded);
            }
            if (size > maxRightGroupSize) {
                maxRightGroupSize = size;
            }
        }

        return CrossJoinModifiedSlotTracker.NULL_COOKIE;
    }

    void startTrackingPrevValues() {
        this.leftRowToSlot.startTrackingPrevValues();
        this.rightRowSetSource.startTrackingPrevValues();
    }

    public void updateLeftRowRedirection(RowSet leftAdded, long slotLocation) {
        if (slotLocation == RowSequence.NULL_ROW_KEY) {
            leftRowToSlot.removeAll(leftAdded);
        } else {
            leftAdded.forAllRowKeys(ii -> leftRowToSlot.putVoid(ii, slotLocation));
        }
    }

    public void onRightGroupInsertion(RowSet rightRowSet, RowSet rightAdded, long slotLocation) {
        // only right side insertions can cause shifts
        final long size = rightRowSet.size();
        final int numBitsNeeded = CrossJoinShiftState.getMinBits(size - 1);
        if (numBitsNeeded > getNumShiftBits()) {
            setNumShiftBitsAndUpdatePrev(numBitsNeeded);
        }
        if (size > maxRightGroupSize) {
            maxRightGroupSize = size;
        }
        // TODO: THIS IS ANOTHER CASE OF UNFORTUNATE VIRTUAL CALLS
        rightAdded.forAllRowKeys(ii -> rightRowToSlot.putVoid(ii, slotLocation));
    }

    public TrackingWritableRowSet getRightRowSet(long slot) {
        TrackingWritableRowSet retVal = rightRowSetSource.get(slot);
        if (retVal == null) {
            retVal = RowSetFactory.empty().toTracking();
        }
        return retVal;
    }

    public TrackingRowSet getPrevRightRowSet(long prevSlot) {
        TrackingRowSet retVal = rightRowSetSource.getPrev(prevSlot);
        if (retVal == null) {
            retVal = RowSetFactory.empty().toTracking();
        }
        return retVal;
    }

    @Override
    public TrackingRowSet getRightRowSetFromLeftRow(long leftRowKey) {
        long slot = leftRowToSlot.get(leftRowKey);
        if (slot == RowSequence.NULL_ROW_KEY) {
            return RowSetFactory.empty().toTracking();
        }
        return getRightRowSet(slot);
    }

    @Override
    public TrackingRowSet getRightRowSetFromPrevLeftRow(long leftRowKey) {
        long slot = leftRowToSlot.getPrev(leftRowKey);
        if (slot == RowSequence.NULL_ROW_KEY) {
            return RowSetFactory.empty().toTracking();
        }
        return getPrevRightRowSet(slot);
    }

    public TrackingWritableRowSet getLeftRowSet(long slot) {
        TrackingWritableRowSet retVal = leftRowSetSource.get(slot);
        if (retVal == null) {
            retVal = RowSetFactory.empty().toTracking();
            if (isLeftTicking) {
                leftRowSetSource.set(slot, retVal);
            }
        }
        return retVal;
    }

    public long getTrackerCookie(long slot) {
        if (slot == EMPTY_RIGHT_SLOT) {
            return -1;
        }
        return modifiedTrackerCookieSource.getUnsafe(slot);
    }

    public long getSlotFromLeftRowKey(long rowKey) {
        return leftRowToSlot.get(rowKey);
    }

    /**
     * @return the number of keys with a slot
     */
    @TestUseOnly
    long liveSlotCount() {
        return hasher.size();
    }

    /**
     * @return one more than the largest slot handed out
     */
    @TestUseOnly
    int slotCapacity() {
        return hasher.idCapacity();
    }

    void clearCookies() {
        for (int si = 0; si < slotCapacity; ++si) {
            modifiedTrackerCookieSource.set(si, CrossJoinModifiedSlotTracker.NULL_COOKIE);
        }
    }

    void validateKeySpaceSize() {
        final long leftLastKey = leftTable.getRowSet().lastRowKey();
        final long rightLastKey = maxRightGroupSize - 1;
        final int minLeftBits = CrossJoinShiftState.getMinBits(leftLastKey);
        final int minRightBits = CrossJoinShiftState.getMinBits(rightLastKey);
        final int numShiftBits = getNumShiftBits();
        if (minLeftBits + numShiftBits > 63) {
            throw new OutOfKeySpaceException("join out of rowSet space (left reqBits + right reservedBits > 63): "
                    + "(left table: {size: " + leftTable.getRowSet().size() + " maxRowKey: " + leftLastKey
                    + " reqBits: " + minLeftBits + "}) X "
                    + "(right table: {maxRowKey: " + rightLastKey + " reqBits: " + minRightBits + " reservedBits: "
                    + numShiftBits + "})"
                    + " exceeds Long.MAX_VALUE. Consider flattening left table or reserving fewer right bits if possible.");
        }
    }
}
