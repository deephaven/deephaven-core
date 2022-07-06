package io.deephaven.engine.table.impl.updateby.hashing;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.LongChunk;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.impl.UpdateByOperator;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * <p>
 * This class tracks indices by individual bucket. During an update cycle it also provides a means to accumulate updates
 * to each bucket so that they can be processed on a per-bucket basis.
 * </p>
 * <br>
 * <p>
 * Accumulation of bucketed changes is done by assigning a cookie, incrementally, to each visited bucket. The cookie can
 * be then used to retrieve the changes to that bucket which lets the update use space proportional to the number of
 * visited buckets in the update, instead of the total number of buckets in the table.
 * </p>
 */
public class UpdateBySlotTracker {
    private static final RowSet EMPTY_INDEX = RowSetFactory.empty();
    private final int chunkSize;

    private long pointer;

    /** how many slots we have allocated */
    private long allocated;

    /** Each time we clear, we add an offset to our cookies, this prevents us from reading old values */
    private long cookieGeneration;

    /** Maintain the cookie for particular visited slots */
    private final LongArraySource slotToCookie = new LongArraySource();

    /** The actual index for the individual slots */
    private final ObjectArraySource<WritableRowSet> slotIndices = new ObjectArraySource<>(WritableRowSet.class);

    /** A source based upon the current cookie for updates to each individual slot. */
    private final ObjectArraySource<UpdateTracker> slotUpdates = new ObjectArraySource<>(UpdateTracker.class);

    private long largestSlotPosition = -1;

    private boolean updatesApplied = false;

    public interface ModifiedSlotConsumer {

        void accept(@NotNull final UpdateTracker tracker, @NotNull final RowSet slotIndex);
    }

    public static class UpdateTracker {

        final int slot;
        // We'll use two sequential builders for this since it'll be better than a single random builder, given that

        // we know both sets of addeds and removeds are going to be processed sequentially.
        RowSetBuilderSequential addedBuilder;
        RowSetBuilderSequential changedBucketAddedBuilder;
        RowSet added;

        RowSetBuilderSequential modifiedBuilder;
        RowSet modified;

        RowSetBuilderSequential removedBuilder;
        RowSetBuilderSequential changedBucketRemovedBuilder;
        RowSet removed;

        boolean affectedByShift = false;

        boolean wasAppendOnly = true;

        private RowSet.SearchIterator buckIt;

        private long smallestModifiedKey;

        public UpdateTracker(final int slot) {
            this.slot = slot;
        }

        public int getSlot() {
            return slot;
        }

        public long getSmallestModifiedKey() {
            return smallestModifiedKey;
        }

        @NotNull
        public RowSet getAdded() {
            if (added == null) {
                WritableRowSet newAdded = null;
                if (addedBuilder != null) {
                    newAdded = addedBuilder.build();
                    addedBuilder = null;
                }

                if (changedBucketAddedBuilder != null) {
                    if (newAdded == null) {
                        newAdded = changedBucketAddedBuilder.build();
                    } else {
                        newAdded.insert(changedBucketAddedBuilder.build());
                    }
                    changedBucketAddedBuilder = null;
                }

                added = (newAdded == null) ? EMPTY_INDEX : newAdded;
            }
            return added;
        }

        @NotNull
        public RowSet getModified() {
            if (modified == null) {
                modified = modifiedBuilder == null ? EMPTY_INDEX : modifiedBuilder.build();
                modifiedBuilder = null;
            }
            return modified;
        }

        @NotNull
        public RowSet getRemoved() {
            if (removed == null) {
                WritableRowSet newRemoved = null;
                if (removedBuilder != null) {
                    newRemoved = removedBuilder.build();
                    removedBuilder = null;
                }

                if (changedBucketRemovedBuilder != null) {
                    if (newRemoved == null) {
                        newRemoved = changedBucketRemovedBuilder.build();
                    } else {
                        newRemoved.insert(changedBucketRemovedBuilder.build());
                    }
                    changedBucketRemovedBuilder = null;
                }

                removed = (newRemoved == null) ? EMPTY_INDEX : newRemoved;
            }
            return removed;
        }

        public boolean wasAppendOnly() {
            return wasAppendOnly;
        }

        public boolean wasShiftOnly() {
            return getRemoved().isEmpty() && getModified().isEmpty() && getAdded().isEmpty() && affectedByShift;
        }

        private void applyTo(@NotNull final WritableRowSet slotindex, @NotNull final RowSetShiftData shifts) {
            final RowSet removed = getRemoved();
            if (removed.isNonempty()) {
                wasAppendOnly = false;
                slotindex.remove(removed);
            }

            if (shifts.nonempty() && affectedByShift && shifts.apply(slotindex)) {
                wasAppendOnly = false;
            }

            final RowSet added = getAdded();
            if (added.isNonempty()) {
                wasAppendOnly &= added.firstRowKey() > slotindex.lastRowKey();
                slotindex.insert(added);
            }

            final RowSet modified = getModified();
            if (modified.isNonempty()) {
                wasAppendOnly = false;
            }

            this.smallestModifiedKey = UpdateByOperator.determineSmallestVisitedKey(added,
                    modified,
                    removed,
                    shifts,
                    slotindex);
        }

        public void setBucketIterator(@NotNull final RowSet.SearchIterator iter) {
            this.buckIt = iter;
        }

        public RowSet.SearchIterator getIterator() {
            return buckIt;
        }
    }

    public UpdateBySlotTracker(final int chunkSize) {
        this.chunkSize = chunkSize;
    }

    public long getModifiedBucketCount() {
        return pointer;
    }

    /**
     * Remove all entries from the tracker.
     */
    public void reset() {
        // Make sure we clear out references to any UpdateTrackers we may have allocated.
        for (int ii = 0; ii < pointer; ii++) {
            slotUpdates.setNull(ii);
        }

        cookieGeneration += pointer;
        if (cookieGeneration > Long.MAX_VALUE / 2) {
            cookieGeneration = 0;
        }
        pointer = 0;
        updatesApplied = false;
    }

    @NotNull
    public RowSet applyUpdates(@NotNull final RowSetShiftData shiftsToApply) {
        Assert.eqFalse(updatesApplied, "updatesApplied");
        updatesApplied = true;
        RowSetBuilderRandom emptiedSlotsBuilder = null;
        slotIndices.ensureCapacity(largestSlotPosition + 1);

        for (int ii = 0; ii < pointer; ii++) {
            final UpdateTracker tracker = slotUpdates.getUnsafe(ii);
            final WritableRowSet slotIndex = slotIndices.getUnsafe(tracker.getSlot());
            emptiedSlotsBuilder = applyUpdateAndTrackEmpty(tracker, slotIndex, shiftsToApply, emptiedSlotsBuilder);
        }

        return emptiedSlotsBuilder == null ? EMPTY_INDEX : emptiedSlotsBuilder.build();
    }

    private RowSetBuilderRandom applyUpdateAndTrackEmpty(@NotNull final UpdateTracker tracker,
            @Nullable final WritableRowSet slotIndex,
            @NotNull final RowSetShiftData shiftsToApply,
            @Nullable RowSetBuilderRandom emptiedSlotsBuilder) {
        if (slotIndex == null) {
            Assert.assertion(tracker.modifiedBuilder == null && tracker.removedBuilder == null,
                    "For a missing slot index the update must have been add only");
            slotIndices.set(tracker.slot, (WritableRowSet) tracker.getAdded());
        } else {
            tracker.applyTo(slotIndex, shiftsToApply);

            if (slotIndex.isEmpty()) {
                if (emptiedSlotsBuilder == null) {
                    emptiedSlotsBuilder = RowSetFactory.builderRandom();
                }
                emptiedSlotsBuilder.addKey(tracker.slot);
            }
        }

        return emptiedSlotsBuilder;
    }

    /**
     * For each value, call slotConsumer.
     *
     * @param slotConsumer the consumer of our values
     */
    public void forAllModifiedSlots(@NotNull final ModifiedSlotConsumer slotConsumer) {
        for (int ii = 0; ii < pointer; ++ii) {
            final UpdateTracker trackerForSlot = slotUpdates.getUnsafe(ii);
            slotConsumer.accept(trackerForSlot, slotIndices.getUnsafe(trackerForSlot.slot));
        }
    }

    /**
     * Add a slot in the main table.
     *
     * @param slot the slot to add.
     */
    public void addToBucket(final int slot,
            @NotNull final LongChunk<? extends RowKeys> addedChunk,
            final int startPos,
            final int length) {
        largestSlotPosition = Math.max(largestSlotPosition, slot);
        final UpdateTracker tracker = getTracker(slot);
        if (tracker.addedBuilder == null) {
            tracker.addedBuilder = RowSetFactory.builderSequential();
        }

        for (int chunkIdx = startPos; chunkIdx < startPos + length; chunkIdx++) {
            tracker.addedBuilder.appendKey(addedChunk.get(chunkIdx));
        }
    }

    public void addToBucket(final int slot,
            final long keyToAdd) {
        largestSlotPosition = Math.max(largestSlotPosition, slot);
        final UpdateTracker tracker = getTracker(slot);
        if (tracker.changedBucketAddedBuilder == null) {
            tracker.changedBucketAddedBuilder = RowSetFactory.builderSequential();
        }

        tracker.changedBucketAddedBuilder.appendKey(keyToAdd);
    }

    public void modifyBucket(final int slot,
            @NotNull final LongChunk<? extends RowKeys> modifiedChunk,
            final int startPos,
            final int length) {
        final UpdateTracker tracker = getTracker(slot);
        if (tracker.modifiedBuilder == null) {
            tracker.modifiedBuilder = RowSetFactory.builderSequential();
        }

        for (int chunkIdx = startPos; chunkIdx < startPos + length; chunkIdx++) {
            tracker.modifiedBuilder.appendKey(modifiedChunk.get(chunkIdx));
        }
    }

    public void modifyBucket(final int slot,
            final long modifiedKey) {
        final UpdateTracker tracker = getTracker(slot);
        if (tracker.modifiedBuilder == null) {
            tracker.modifiedBuilder = RowSetFactory.builderSequential();
        }
        tracker.modifiedBuilder.appendKey(modifiedKey);
    }

    public void removeFromBucket(final int slot,
            @NotNull final LongChunk<? extends RowKeys> removedChunk,
            final int startPos,
            final int length) {
        final UpdateTracker tracker = getTracker(slot);
        if (tracker.removedBuilder == null) {
            tracker.removedBuilder = RowSetFactory.builderSequential();
        }
        for (int chunkIdx = startPos; chunkIdx < startPos + length; chunkIdx++) {
            tracker.removedBuilder.appendKey(removedChunk.get(chunkIdx));
        }
    }

    public void removeFromBucket(final int slot,
            final long removedKey) {
        final UpdateTracker tracker = getTracker(slot);
        if (tracker.changedBucketRemovedBuilder == null) {
            tracker.changedBucketRemovedBuilder = RowSetFactory.builderSequential();
        }
        tracker.changedBucketRemovedBuilder.appendKey(removedKey);
    }

    public void markForShift(int slot) {
        final UpdateTracker tracker = getTracker(slot);
        tracker.affectedByShift = true;
    }

    public UpdateTracker getTracker(final int slot) {
        long cookie = slotToCookie.getLong(slot);
        if (!isValidCookie(cookie)) {
            cookie = createUpdateForSlot(slot);
        }

        final long pointer = getPointerFromCookie(cookie);
        return slotUpdates.getUnsafe(pointer);
    }

    private long createUpdateForSlot(final int slot) {
        if (pointer == allocated) {
            allocated += chunkSize;
            slotUpdates.ensureCapacity(allocated);
        }

        final long cookie = getCookieFromPointer(pointer);
        slotToCookie.ensureCapacity(allocated);
        slotToCookie.set(slot, cookie);
        slotUpdates.set(pointer, new UpdateTracker(slot));
        pointer++;

        return cookie;
    }

    /**
     * Is this cookie within our valid range (greater than or equal to our generation, but less than the pointer after
     * adjustment?
     *
     * @param cookie the cookie to check for validity
     *
     * @return true if the cookie is from the current generation, and references a valid slot in our table
     */
    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private boolean isValidCookie(long cookie) {
        return cookie >= cookieGeneration && getPointerFromCookie(cookie) < pointer;
    }

    /**
     * Get a cookie to return to the user, given a pointer value.
     *
     * @param pointer the pointer to convert to a cookie
     * @return the cookie to return to the user
     */
    private long getCookieFromPointer(long pointer) {
        return cookieGeneration + pointer;
    }

    /**
     * Given a valid user's cookie, return the corresponding pointer.
     *
     * @param cookie the valid cookie
     * @return the pointer into modifiedSlots
     */
    private long getPointerFromCookie(long cookie) {
        return cookie - cookieGeneration;
    }
}
