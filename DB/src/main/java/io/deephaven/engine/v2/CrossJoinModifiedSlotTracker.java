/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.tables.SortingOrder;
import io.deephaven.engine.v2.sort.LongSortKernel;
import io.deephaven.engine.v2.sources.ObjectArraySource;
import io.deephaven.engine.v2.sources.chunk.Attributes.*;
import io.deephaven.engine.v2.sources.chunk.ChunkType;
import io.deephaven.engine.v2.sources.chunk.WritableLongChunk;
import io.deephaven.engine.v2.sources.chunk.sized.SizedLongChunk;
import io.deephaven.engine.v2.utils.*;
import io.deephaven.engine.structures.RowSequence;

import java.util.function.Consumer;

/**
 * A tracker for modified cross join hash table slots.
 *
 * After adding an entry, you get back a cookie, which must be passed in on future modification operations for that
 * slot.
 */
class CrossJoinModifiedSlotTracker {
    static final long NULL_COOKIE = 0;

    private static final int START_SLOT_CHUNK_SIZE = 256;
    private static final int CHUNK_SIZE = 4096;

    private int maxSlotChunkCapacity = START_SLOT_CHUNK_SIZE;

    IndexShiftData leftShifted;
    IndexShiftData rightShifted;

    TrackingMutableRowSet leftAdded;
    TrackingMutableRowSet leftRemoved;
    TrackingMutableRowSet leftModified;

    boolean hasLeftModifies = false;
    boolean hasRightModifies = false;
    boolean finishedRightProcessing = false;

    private final RightIncrementalChunkedCrossJoinStateManager jsm;

    CrossJoinModifiedSlotTracker(RightIncrementalChunkedCrossJoinStateManager jsm) {
        this.jsm = jsm;
    }

    class SlotState {
        long cookie;
        long slotLocation;
        boolean rightChanged = false;
        boolean finalizedRight = false;
        boolean needsRightShift = false;
        boolean isLeftShifted = false;

        int chunkCapacity = START_SLOT_CHUNK_SIZE;
        final SizedLongChunk<Values> flagChunk = new SizedLongChunk<>();
        final SizedLongChunk<RowKeys> keyChunk = new SizedLongChunk<>();
        RowSetBuilderRandom indexBuilder = RowSetFactoryImpl.INSTANCE.getRandomBuilder();

        long lastIndex = 0; // if added/removed/modified have been shifted then this is the left-rowSet for the shift
        TrackingMutableRowSet rightAdded;
        TrackingMutableRowSet rightRemoved;
        TrackingMutableRowSet rightModified;
        IndexShiftData innerShifted;

        TrackingMutableRowSet leftRowSet; // reference, NOT a copy
        TrackingMutableRowSet rightRowSet; // reference, NOT a copy

        private SlotState() {
            keyChunk.ensureCapacityPreserve(START_SLOT_CHUNK_SIZE);
            flagChunk.ensureCapacityPreserve(START_SLOT_CHUNK_SIZE);
        }

        private void clear() {
            if (finalizedRight) {
                rightAdded.close();
                rightRemoved.close();
                rightModified.close();
            }
            keyChunk.close();
            flagChunk.close();
        }

        private SlotState needsRightShift() {
            needsRightShift = true;
            return this;
        }

        private SlotState applyLeftShift() {
            if (isLeftShifted) {
                return this;
            }

            isLeftShifted = true;
            leftShifted.apply(leftRowSet);
            return this;
        }

        private void ensureChunkCapacityRemaining() {
            if (keyChunk.get().size() < chunkCapacity) {
                return;
            }

            final int originalCapacity = chunkCapacity;
            chunkCapacity = (chunkCapacity >= CHUNK_SIZE) ? chunkCapacity + CHUNK_SIZE : 2 * chunkCapacity;
            maxSlotChunkCapacity = Math.max(maxSlotChunkCapacity, chunkCapacity);
            keyChunk.ensureCapacityPreserve(chunkCapacity);
            flagChunk.ensureCapacityPreserve(chunkCapacity);
        }

        private SlotState appendToChunk(final long key, final byte flag) {
            ensureChunkCapacityRemaining();
            keyChunk.get().add(key);
            flagChunk.get().add(flag);
            return this;
        }

        private SlotState appendToBuilder(final long key) {
            indexBuilder.addKey(key);
            return this;
        }

        private void doFinalizeRightState() {
            if (finalizedRight) {
                return;
            }
            finalizedRight = true;

            ensureSortKernel();
            final WritableLongChunk<RowKeys> keyChunk = this.keyChunk.get();
            final WritableLongChunk<Values> flagChunk = this.flagChunk.get();
            sortKernel.sort(WritableLongChunk.downcast(flagChunk), keyChunk);

            rightChanged = keyChunk.size() > 0;

            // finalize right rowSet; transform from right rowSet to downstream offset
            final RowSetBuilderSequential innerAdded = RowSetFactoryImpl.INSTANCE.getSequentialBuilder();
            final RowSetBuilderSequential innerRemoved = RowSetFactoryImpl.INSTANCE.getSequentialBuilder();

            // let's accumulate downstream offsets too
            final RowSetBuilderSequential removedBuilder = RowSetFactoryImpl.INSTANCE.getSequentialBuilder();

            try (final RowSequence.Iterator iter = rightRowSet.getRowSequenceIterator()) {
                final long startRelativePos = iter.getRelativePosition();

                // first we build and translate the removed
                for (int ii = 0; ii < keyChunk.size(); ++ii) {
                    final long key = keyChunk.get(ii);
                    final long flag = flagChunk.get(ii);
                    if (flag == FLAG_RM) {
                        innerRemoved.appendKey(key);
                        // translate the removed
                        if (iter.hasMore()) {
                            iter.advance(key);
                        }
                        final long downstreamOffset = iter.getRelativePosition() - startRelativePos;
                        keyChunk.set(ii, downstreamOffset);
                        removedBuilder.appendKey(downstreamOffset);
                    } else if (flag == FLAG_ADD) {
                        innerAdded.appendKey(key);
                    }
                }
            }

            // make our right rowSet be what it needs to be
            final long oldRightSize = rightRowSet.size();
            try (final TrackingMutableRowSet added = innerAdded.build();
                 final TrackingMutableRowSet removed = innerRemoved.build()) {
                rightRowSet.remove(removed);

                // then we shift
                if (needsRightShift) {
                    rightShifted.apply(rightRowSet);
                }

                if (added.isNonempty()) {
                    rightRowSet.insert(added);
                    jsm.onRightGroupInsertion(rightRowSet, added, slotLocation);
                }

                Assert.eq(oldRightSize + added.size() - removed.size(), "oldRightSize + added.size() - removed.size()",
                        rightRowSet.size(), "rightRowSet.size()");
            }

            // now translate added && modified; accumulate them too
            final RowSetBuilderSequential addedBuilder = RowSetFactoryImpl.INSTANCE.getSequentialBuilder();
            final RowSetBuilderSequential modifiedBuilder = RowSetFactoryImpl.INSTANCE.getSequentialBuilder();

            try (final RowSequence.Iterator iter = rightRowSet.getRowSequenceIterator()) {
                final long startRelativePos = iter.getRelativePosition();

                for (int ii = 0; ii < keyChunk.size(); ++ii) {
                    final long key = keyChunk.get(ii);
                    final long flag = flagChunk.get(ii);
                    if (flag == FLAG_RM) {
                        continue;
                    }

                    if (iter.hasMore()) {
                        iter.advance(key);
                    }
                    final long downstreamOffset = iter.getRelativePosition() - startRelativePos;
                    keyChunk.set(ii, downstreamOffset);
                    if (flag == FLAG_ADD) {
                        addedBuilder.appendKey(downstreamOffset);
                    } else if (flag == FLAG_MOD) {
                        modifiedBuilder.appendKey(downstreamOffset);
                    } else {
                        throw new IllegalStateException(
                                "CrossJoinModifiedSlotTracker encountered unexpected flag value: " + flag);
                    }
                }
            }

            // generate downstream updates
            long shiftDelta = 0;
            int preIdx = 0, postIdx = 0;
            long preOff = 0, postOff = 0;
            final IndexShiftData.Builder shiftBuilder = new IndexShiftData.Builder();

            while (preIdx < keyChunk.size() && flagChunk.get(preIdx) != FLAG_RM) {
                ++preIdx;
            }
            while (postIdx < keyChunk.size() && flagChunk.get(postIdx) != FLAG_ADD) {
                ++postIdx;
            }

            final long newRightSize = rightRowSet.size();
            while (postOff < newRightSize && preOff < oldRightSize) {
                final long preNextOff = (preIdx == keyChunk.size()) ? oldRightSize : keyChunk.get(preIdx);
                final long postNextOff = (postIdx == keyChunk.size()) ? newRightSize : keyChunk.get(postIdx);

                final long canShift = Math.min(preNextOff - preOff, postNextOff - postOff);
                if (canShift > 0) {
                    shiftBuilder.shiftRange(preOff, preOff + canShift - 1, shiftDelta);
                    preOff += canShift;
                    postOff += canShift;
                }
                if (canShift < 0) {
                    throw new IllegalStateException();
                }

                if (preOff == preNextOff && preIdx < keyChunk.size()) {
                    ++preOff;
                    --shiftDelta;
                    for (++preIdx; preIdx < keyChunk.size(); ++preIdx) {
                        if (flagChunk.get(preIdx) == FLAG_RM) {
                            break;
                        }
                    }
                }

                if (postOff == postNextOff && postIdx < keyChunk.size()) {
                    ++postOff;
                    ++shiftDelta;
                    for (++postIdx; postIdx < keyChunk.size(); ++postIdx) {
                        if (flagChunk.get(postIdx) == FLAG_ADD) {
                            break;
                        }
                    }
                }
            }

            rightAdded = addedBuilder.build();
            rightRemoved = removedBuilder.build();
            rightModified = modifiedBuilder.build();
            innerShifted = shiftBuilder.build();
            hasRightModifies |= rightModified.isNonempty();

            keyChunk.setSize(0);
            flagChunk.setSize(0);
        }
    }

    private final ObjectArraySource<SlotState> modifiedSlots = new ObjectArraySource<>(SlotState.class);
    private LongSortKernel<RowKeys, RowKeys> sortKernel;

    private void ensureSortKernel() {
        if (sortKernel == null) {
            sortKernel = LongSortKernel.makeContext(ChunkType.Long, SortingOrder.Ascending, maxSlotChunkCapacity, true);
        }
    }

    /**
     * the location that we must write to in modified slots; also if we have a pointer that falls outside the range [0,
     * pointer); then we know it is invalid
     */
    private long pointer;
    /** how many slots we have allocated */
    private long allocated;
    /** Each time we clear, we add an offset to our cookies, this prevents us from reading old values */
    private long cookieGeneration = 128;

    private static final byte FLAG_ADD = 0x1;
    private static final byte FLAG_RM = 0x2;
    private static final byte FLAG_MOD = 0x4;

    /**
     * Remove all entries from the tracker.
     *
     * @return Whether all externally-stored cookies should be reset to {@link #NULL_COOKIE}
     **/
    boolean clear() {
        boolean needToResetCookies = false;
        cookieGeneration += pointer;
        if (cookieGeneration > Long.MAX_VALUE / 2) {
            cookieGeneration = 0;
            needToResetCookies = true;
        }
        for (long i = 0; i < pointer; ++i) {
            if (modifiedSlots.get(i) == null) {
                continue;
            }
            modifiedSlots.get(i).clear();
            modifiedSlots.set(i, null);
        }
        maxSlotChunkCapacity = START_SLOT_CHUNK_SIZE;
        pointer = 0;
        if (sortKernel != null) {
            sortKernel.close();
            sortKernel = null;
        }
        leftShifted = null;
        rightShifted = null;

        // leftAdded/leftRemoved/leftModified are used by the downstream update; so we do not free them
        leftAdded = null;
        leftRemoved = null;
        leftModified = null;

        hasLeftModifies = false;
        hasRightModifies = false;
        finishedRightProcessing = false;

        return needToResetCookies;
    }

    /**
     * Is this cookie within our valid range (greater than or equal to our generation, but less than the pointer after
     * adjustment?
     *
     * @param cookie the cookie to check for validity
     *
     * @return true if the cookie is from the current generation, and references a valid slot in our table
     */
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

    private SlotState getSlotState(final long cookie, final long slot) {
        final SlotState state;
        if (!isValidCookie(cookie)) {
            if (pointer == allocated) {
                allocated += CHUNK_SIZE;
                modifiedSlots.ensureCapacity(allocated);
            }
            state = new SlotState();
            modifiedSlots.set(pointer, state);
            state.slotLocation = slot;
            state.cookie = getCookieFromPointer(pointer++);

            state.leftRowSet = jsm.getLeftIndex(slot);
            state.rightRowSet = jsm.getRightIndex(slot);

            if (finishedRightProcessing) {
                state.finalizedRight = true;
                state.rightAdded = state.rightRemoved = state.rightModified = RowSetFactoryImpl.INSTANCE.getEmptyRowSet();
                state.innerShifted = IndexShiftData.EMPTY;
            }
        } else {
            state = modifiedSlots.get(getPointerFromCookie(cookie));
            Assert.eq(state.slotLocation, "state.slotLocation", slot, "slot");
        }
        return state;
    }

    /**
     * Move a main table location.
     *
     * @param newTableLocation the new hash slot
     */
    void moveTableLocation(long cookie, long newTableLocation) {
        if (isValidCookie(cookie)) {
            final long pointer = getPointerFromCookie(cookie);
            final SlotState state = modifiedSlots.get(pointer);
            state.slotLocation = newTableLocation;
        }
    }

    long appendChunkAdd(final long cookie, final long slot, final long key) {
        return getSlotState(cookie, slot).appendToChunk(key, FLAG_ADD).cookie;
    }

    long appendChunkRemove(final long cookie, final long slot, final long key) {
        return getSlotState(cookie, slot).appendToChunk(key, FLAG_RM).cookie;
    }

    long appendChunkModify(final long cookie, final long slot, final long key) {
        return getSlotState(cookie, slot).appendToChunk(key, FLAG_MOD).cookie;
    }

    long appendToBuilder(final long cookie, final long slot, final long leftIndex) {
        return getSlotState(cookie, slot).appendToBuilder(leftIndex).cookie;
    }

    // Right shifts cannot be applied until after the removes are applied to the slot's right rowSet. So, we ensure that
    // a tracker-slot is allocated for each slot affected by a shift, and apply the shifts later.
    long needsRightShift(final long cookie, final long slot) {
        return getSlotState(cookie, slot).needsRightShift().cookie;
    }

    // Left shifts can actually be applied immediately (removes are already rm'd), but we need to be careful to only
    // shift the leftRowSet once.
    long needsLeftShift(final long cookie, final long slot) {
        return getSlotState(cookie, slot).applyLeftShift().cookie;
    }

    SlotState getFinalSlotState(long cookie) {
        if (!isValidCookie(cookie)) {
            return null;
        }
        ensureSortKernel();

        final SlotState state = modifiedSlots.get(getPointerFromCookie(cookie));
        if (state != null) {
            state.doFinalizeRightState();
        }
        return state;
    }

    void forAllModifiedSlots(Consumer<SlotState> callback) {
        for (int ii = 0; ii < pointer; ++ii) {
            final SlotState slotState = modifiedSlots.get(ii);
            if (slotState != null) {
                slotState.doFinalizeRightState();
                callback.accept(slotState);
            }
        }
    }

    void finalizeRightProcessing() {
        for (int ii = 0; ii < pointer; ++ii) {
            final SlotState slotState = modifiedSlots.get(ii);
            if (slotState != null) {
                slotState.doFinalizeRightState();
            }
        }
        finishedRightProcessing = true;
    }

    void flushLeftRemoves() {
        final RowSetBuilderRandom builder = RowSetFactoryImpl.INSTANCE.getRandomBuilder();
        for (int jj = 0; jj < pointer; ++jj) {
            final SlotState slotState = modifiedSlots.get(jj);
            if (slotState == null) {
                continue;
            }

            final TrackingMutableRowSet leftRemoved = slotState.indexBuilder.build();
            slotState.leftRowSet.remove(leftRemoved);
            slotState.indexBuilder = RowSetFactoryImpl.INSTANCE.getRandomBuilder();
            long sizePrev = slotState.rightRowSet.sizePrev();
            if (sizePrev > 0) {
                leftRemoved.forAllLongs(ii -> {
                    final long prevOffset = ii << jsm.getPrevNumShiftBits();
                    builder.addRange(prevOffset, prevOffset + sizePrev - 1);
                });
            }
        }
        leftRemoved = builder.build();
    }

    void flushLeftAdds() {
        final RowSetBuilderRandom downstreamAdds = RowSetFactoryImpl.INSTANCE.getRandomBuilder();
        for (int jj = 0; jj < pointer; ++jj) {
            final SlotState slotState = modifiedSlots.get(jj);
            if (slotState == null) {
                continue;
            }

            final TrackingMutableRowSet leftAdded = slotState.indexBuilder.build();
            slotState.leftRowSet.insert(leftAdded);
            jsm.updateLeftRedirectionIndex(leftAdded, slotState.slotLocation);

            slotState.indexBuilder = null;
            long size = slotState.rightRowSet.size();
            if (size > 0) {
                leftAdded.forAllLongs(ii -> {
                    final long currOffset = ii << jsm.getNumShiftBits();
                    downstreamAdds.addRange(currOffset, currOffset + size - 1);
                });
            }

            final RowSetBuilderRandom modifiedAdds = RowSetFactoryImpl.INSTANCE.getRandomBuilder();
            for (int ii = 0; ii < slotState.keyChunk.get().size(); ++ii) {
                final long key = slotState.keyChunk.get().get(ii);
                if (slotState.flagChunk.get().get(ii) != FLAG_ADD) {
                    continue;
                }
                // must be a modify
                if (size > 0) {
                    final long currOffset = key << jsm.getNumShiftBits();
                    downstreamAdds.addRange(currOffset, currOffset + size - 1);
                }
                modifiedAdds.addKey(key);
            }
            try (final TrackingMutableRowSet moreLeftAdded = modifiedAdds.build()) {
                slotState.leftRowSet.insert(moreLeftAdded);
                jsm.updateLeftRedirectionIndex(moreLeftAdded, slotState.slotLocation);
            }
        }
        if (leftAdded == null) {
            leftAdded = downstreamAdds.build();
        } else {
            try (final TrackingMutableRowSet toInsert = downstreamAdds.build()) {
                leftAdded.insert(toInsert);
            }
        }
    }

    void flushLeftModifies() {
        // Removes accumulate in the slot builder, modifies and adds accumulate in the chunks.
        // We leave the adds to process after left shifts are handled.

        final RowSetBuilderRandom rmBuilder = RowSetFactoryImpl.INSTANCE.getRandomBuilder();
        final RowSetBuilderRandom modBuilder = RowSetFactoryImpl.INSTANCE.getRandomBuilder();
        for (int jj = 0; jj < pointer; ++jj) {
            final SlotState slotState = modifiedSlots.get(jj);
            if (slotState == null) {
                continue;
            }
            final TrackingMutableRowSet leftRemoved = slotState.indexBuilder.build();
            slotState.leftRowSet.remove(leftRemoved);
            jsm.updateLeftRedirectionIndex(leftRemoved, TrackingMutableRowSet.NULL_ROW_KEY);
            slotState.indexBuilder = RowSetFactoryImpl.INSTANCE.getRandomBuilder();
            final long sizePrev = slotState.rightRowSet.sizePrev();
            if (sizePrev > 0) {
                leftRemoved.forAllLongs(ii -> {
                    final long prevOffset = ii << jsm.getPrevNumShiftBits();
                    rmBuilder.addRange(prevOffset, prevOffset + sizePrev - 1);
                });
            }
            final long size = slotState.rightRowSet.size();
            if (sizePrev > 0 && size > 0) {
                for (int ii = 0; ii < slotState.keyChunk.get().size(); ++ii) {
                    final long key = slotState.keyChunk.get().get(ii);
                    if (slotState.flagChunk.get().get(ii) != FLAG_MOD) {
                        continue;
                    }
                    // must be a modify
                    final long currOffset = key << jsm.getNumShiftBits();
                    modBuilder.addRange(currOffset, currOffset + size - 1);
                }
            }
        }
        try (final TrackingMutableRowSet toRemove = rmBuilder.build()) {
            leftRemoved.insert(toRemove);
        }
        leftModified = modBuilder.build();
        hasLeftModifies = leftModified.isNonempty();
    }
}
