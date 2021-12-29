package io.deephaven.engine.table.impl.by;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.impl.sort.timsort.LongLongTimsortKernel;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.table.impl.util.*;
import org.jetbrains.annotations.NotNull;

/**
 * <p>
 * A tracker for accumulating changes to aggregation states for {@link Table#groupBy}.
 *
 * <p>
 * The tracker is used in the initial (insert only) build phase, as well as in subsequent update passes.
 *
 * <p>
 * Update processing is performed as follows (note that flags are accumulated across <em>steps 1-4</em> and used in
 * <em>step 5</em>):
 * <ol>
 * <li>Probe and accumulate removes (including modified-pre-shift when key columns are modified) in sequential builders
 * per state, then build the removed {@link TrackingWritableRowSet} for each state and remove it from the state's
 * {@link TrackingWritableRowSet}</li>
 * <li>Probe shifts and apply them as they are found to impact a given state's {@link TrackingWritableRowSet}, writing
 * down the total number of states with shifts as the chunk size for accumulating shifts in <em>step 5</em></li>
 * <li>Probe non-key modifies and flag impacted states</li>
 * <li>Build and accumulate adds (including modified-post-shift when key columns are modified) in sequential builders
 * per state, then build the added {@link TrackingWritableRowSet} for each state and add it to the state's
 * {@link TrackingWritableRowSet}</li>
 * <li>Update redirections from the previous {@link TrackingWritableRowSet} first key to the current
 * {@link TrackingWritableRowSet} first key, and from old slot to new slot where a state was moved or promoted in
 * rehash, accumulating row keys in 3 random builders (for added, removed, and modified) and shifts in a pair of
 * parallel {@link WritableLongChunk}s for previous and current, using the following logic:
 * <ol>
 * <li>Non-empty to empty transitions as removes of the previous first key</li>
 * <li>Empty or null placeholder to non-empty transitions as adds of the current first key</li>
 * <li>Shifted-only states as shifts from previous first key to current first key, appended to the paired shift
 * chunks</li>
 * <lI>All other changes as modifies if first key is unchanged, else paired removes and adds if first key changed</lI>
 * </ol>
 * </li>
 * <li>Sort the shift chunks by the previous keys, accumulate shifts into a {@link RowSetShiftData.Builder}</li>
 * </ol>
 *
 * <p>
 * In each phase, the initial addition of a state to the tracker will return a cookie, which must be passed to
 * subsequent updates to the tracker for that state.
 *
 * <p>
 * To process results after steps 1, 4, and 5, the caller uses
 * {@link #applyRemovesToStates(ObjectArraySource, ObjectArraySource)},
 * {@link #applyAddsToStates(ObjectArraySource, ObjectArraySource)}, and
 * {@link #makeUpdateFromStates(ObjectArraySource, ObjectArraySource, WritableRowSet, WritableRowRedirection, ModifiedColumnSetProducer)}
 * respectively.
 */
class IncrementalByAggregationUpdateTracker {

    static final long NULL_COOKIE = 0;

    private static final long MINIMUM_COOKIE = 1;

    private static final int ALLOCATION_UNIT = 4096;

    /**
     * For each updated state, store the slot its in (regardless of whether main or overflow) in the higher 7 bytes, and
     * flags in the lower 1 byte. Note that flags only use 5 bits currently, but it seems reasonable to reserve a whole
     * byte.
     */
    private final LongArraySource updatedStateSlotAndFlags = new LongArraySource();

    /**
     * Builders (used in remove processing and add processing), parallel to {@code updatedStateSlotAndFlags}.
     */
    private final ObjectArraySource<RowSetBuilderSequential> builders =
            new ObjectArraySource<>(RowSetBuilderSequential.class);

    /**
     * Each time we clear, we add an offset to our cookies, this prevents us from reading old values.
     */
    private long cookieGeneration = MINIMUM_COOKIE;

    /**
     * The number of updated states, which is also the next position we will use in {@code updateStateSlotAndFlags} and
     * {@code builders}. Note that cookies with implied pointers outside of {@code [0, size)} are known to be invalid.
     */
    private int size;

    /**
     * The number of tracker positions allocated.
     */
    private int capacity;

    /**
     * <p>
     * The set of positions in {@link #updatedStateSlotAndFlags} (and possibly {@link #builders}) that have been updated
     * in the current pass. Each corresponding "slot and flags" value will have the {@link #FLAG_STATE_IN_CURRENT_PASS}
     * bit set.
     * <p>
     * Note that current pass membership is recorded by {@link #processShift(long, int, long)} and
     * {@link #processAdd(long, int, long)}, only, and cleared in the following
     * {@link #applyAddsToStates(ObjectArraySource, ObjectArraySource)} or
     * {@link #applyShiftToStates(ObjectArraySource, ObjectArraySource, long, long, long)}.
     */
    private final IntegerArraySource currentPassPositions = new IntegerArraySource();

    /**
     * The number of states whose "slot and flags" position can be found in in {@link #currentPassPositions}.
     */
    private int currentPassSize;

    /**
     * The number of "current pass" positions allocated.
     */
    private int currentPassCapacity;

    // @formatter:off
    private static final int FLAG_SHIFT                  = 8;
    private static final int FLAG_MASK                   = 0b11111111;
    private static final byte FLAG_STATE_IN_CURRENT_PASS = 0b00000001;
    private static final byte FLAG_STATE_HAS_REMOVES     = 0b00000010;
    private static final byte FLAG_STATE_HAS_SHIFTS      = 0b00000100;
    private static final byte FLAG_STATE_HAS_MODIFIES    = 0b00001000;
    private static final byte FLAG_STATE_HAS_ADDS        = 0b00010000;
    // @formatter:on

    /**
     * Remove all states from the tracker.
     *
     * @return Whether all externally-stored cookies should be reset to {@link #NULL_COOKIE}
     */
    boolean clear() {
        boolean needToResetCookies = false;
        cookieGeneration += size;
        if (cookieGeneration > Long.MAX_VALUE / 2) {
            cookieGeneration = MINIMUM_COOKIE;
            needToResetCookies = true;
        }
        size = 0;
        return needToResetCookies;
    }

    /**
     * Get the size of this tracker, meaning the number of states with recorded updates.
     *
     * @return The size of the tracker
     */
    int size() {
        return size;
    }

    /**
     * Is this cookie within our valid range (greater than or equal to our generation, but less than the size after
     * adjustment)?
     *
     * @param cookie The cookie to check for validity
     * @return true if the cookie is from the current generation,and references a valid tracker position
     */
    private boolean isValidCookie(final long cookie) {
        return cookie >= cookieGeneration && cookieToPosition(cookie) < size;
    }

    /**
     * Given a position value, get a cookie for the state to store.
     *
     * @param position the position to convert to a cookie
     * @return the cookie to return to the user
     */
    private long positionToCookie(final int position) {
        return cookieGeneration + position;
    }

    /**
     * Given a state's valid cookie, get the corresponding position.
     *
     * @param cookie Ghe valid cookie
     * @return Ghe position in the tracker
     */
    private int cookieToPosition(long cookie) {
        return (int) (cookie - cookieGeneration);
    }

    /**
     * Record that a row key has been added to a state on initial build, to be applied in
     * {@link #applyAddsAndMakeInitialRowSet(ObjectArraySource, ObjectArraySource, WritableRowRedirection)}
     *
     * @param cookie The last known cookie for the state
     * @param stateSlot The state's slot (in main table space)
     * @param addedIndex The row key that was added
     * @return The new cookie for the state if it has changed
     */
    final long processInitialAdd(final long cookie, final int stateSlot, final long addedIndex) {
        return setFlagsAndBuild(cookie, stateSlot, FLAG_STATE_HAS_ADDS, addedIndex);
    }

    /**
     * Record that a row key has been removed from a state, to be applied in
     * {@link #applyRemovesToStates(ObjectArraySource, ObjectArraySource)}.
     *
     * @param cookie The last known cookie for the state
     * @param stateSlot The state's slot (in main table space)
     * @param removedIndex The row key that was removed
     * @return The new cookie for the state if it has changed
     */
    final long processRemove(final long cookie, final int stateSlot, final long removedIndex) {
        return setFlagsAndBuild(cookie, stateSlot, FLAG_STATE_HAS_REMOVES, removedIndex);
    }

    /**
     * Record that a row key has been shifted in a state, to be applied in
     * {@link #applyShiftToStates(ObjectArraySource, ObjectArraySource, long, long, long)}.
     *
     * @param cookie The last known cookie for the state
     * @param stateSlot The state's slot (in main table space)
     * @param unusedShiftedIndex Unused shifted row key argument, so we can use a method reference with the right
     *        signature
     * @return The new cookie for the state if it has changed
     */
    final long processShift(final long cookie, final int stateSlot,
            @SuppressWarnings("unused") final long unusedShiftedIndex) {
        return setFlags(cookie, stateSlot, (byte) (FLAG_STATE_HAS_SHIFTS | FLAG_STATE_IN_CURRENT_PASS));
    }

    /**
     * Record that a row key has been shifted in a state, already applied.
     *
     * @param cookie The last known cookie for the state
     * @param stateSlot The state's slot (in main table space)
     * @return The new cookie for the state if it has changed
     */
    final long processAppliedShift(final long cookie, final int stateSlot) {
        return setFlags(cookie, stateSlot, FLAG_STATE_HAS_SHIFTS);
    }

    /**
     * Record that a row key has been modified in a state.
     *
     * @param cookie The last known cookie for the state
     * @param stateSlot The state's slot (in main table space)
     * @param unusedModifiedIndex Unused modified row key argument, so we can use a method reference with the right
     *        signature
     * @return The new cookie for the state if it has changed
     */
    final long processModify(final long cookie, final int stateSlot,
            @SuppressWarnings("unused") final long unusedModifiedIndex) {
        return setFlags(cookie, stateSlot, FLAG_STATE_HAS_MODIFIES);
    }

    /**
     * Record that a row key has been added to a state, to be applied in
     * {@link #applyAddsToStates(ObjectArraySource, ObjectArraySource)}.
     *
     * @param cookie The last known cookie for the state
     * @param stateSlot The state's slot (in main table space)
     * @param addedIndex The row key that was added
     * @return The new cookie for the state if it has changed
     */
    final long processAdd(final long cookie, final int stateSlot, final long addedIndex) {
        return setFlagsAndBuild(cookie, stateSlot, (byte) (FLAG_STATE_HAS_ADDS | FLAG_STATE_IN_CURRENT_PASS),
                addedIndex);
    }

    /**
     * Move a state to a new main or overflow table location.
     *
     * @param cookie The last known cookie for the state
     * @param newStateSlot The state's slot (in main table space)
     */
    final void processStateMove(final long cookie, final int newStateSlot) {
        if (isValidCookie(cookie)) {
            final long position = cookieToPosition(cookie);
            final long currentSlotAndFlags = updatedStateSlotAndFlags.getLong(position);
            final long resultSlotAndFlags = ((long) newStateSlot << FLAG_SHIFT) | (currentSlotAndFlags & FLAG_MASK);
            updatedStateSlotAndFlags.set(position, resultSlotAndFlags);
        }
    }

    private long setFlagsAndBuild(final long cookie, final int stateSlot, final byte flags, final long index) {
        final int position;
        final long resultCookie;
        final long currentSlotAndFlags;
        if (isValidCookie(cookie)) {
            position = cookieToPosition(cookie);
            resultCookie = cookie;
            currentSlotAndFlags = updatedStateSlotAndFlags.getLong(position);
        } else {
            checkCapacity();
            position = size++;
            resultCookie = positionToCookie(position);
            currentSlotAndFlags = 0L;
        }
        final RowSetBuilderSequential builder;
        final long resultSlotAndFlags = ((long) stateSlot << FLAG_SHIFT) | (currentSlotAndFlags & FLAG_MASK | flags);
        if (currentSlotAndFlags != resultSlotAndFlags) {
            updatedStateSlotAndFlags.set(position, resultSlotAndFlags);
            if ((flags & FLAG_STATE_IN_CURRENT_PASS) != 0 && (currentSlotAndFlags & FLAG_STATE_IN_CURRENT_PASS) == 0) {
                checkCurrentPassCapacity();
                currentPassPositions.set(currentPassSize++, position);
            }
            builders.set(position, builder = RowSetFactory.builderSequential());
        } else {
            builder = builders.get(position);
        }
        // noinspection ConstantConditions
        builder.appendKey(index);
        return resultCookie;
    }

    private long setFlags(final long cookie, final int stateSlot, final byte flags) {
        final int position;
        final long resultCookie;
        final long currentSlotAndFlags;
        if (isValidCookie(cookie)) {
            position = cookieToPosition(cookie);
            resultCookie = cookie;
            currentSlotAndFlags = updatedStateSlotAndFlags.getLong(position);
        } else {
            checkCapacity();
            position = size++;
            resultCookie = positionToCookie(position);
            currentSlotAndFlags = 0L;
        }
        final long resultSlotAndFlags = ((long) stateSlot << FLAG_SHIFT) | (currentSlotAndFlags & FLAG_MASK | flags);
        if (currentSlotAndFlags != resultSlotAndFlags) {
            updatedStateSlotAndFlags.set(position, resultSlotAndFlags);
            if ((flags & FLAG_STATE_IN_CURRENT_PASS) != 0 && (currentSlotAndFlags & FLAG_STATE_IN_CURRENT_PASS) == 0) {
                checkCurrentPassCapacity();
                currentPassPositions.set(currentPassSize++, position);
            }
        }
        return resultCookie;
    }

    private void checkCapacity() {
        if (size == capacity) {
            capacity += ALLOCATION_UNIT;
            updatedStateSlotAndFlags.ensureCapacity(capacity);
            builders.ensureCapacity(capacity);
        }
    }

    private void checkCurrentPassCapacity() {
        if (currentPassSize == currentPassCapacity) {
            currentPassCapacity += ALLOCATION_UNIT;
            currentPassPositions.ensureCapacity(currentPassCapacity);
        }
    }

    /**
     * Apply accumulated adds to their states, populate the result {@link WritableRowRedirection}, and build the initial
     * result {@link TrackingWritableRowSet}.
     *
     * @param rowSetSource The {@link TrackingWritableRowSet} column source for the main table
     * @param overflowRowSetSource The {@link TrackingWritableRowSet} column source for the overflow table
     * @param rowRedirection The result {@link WritableRowRedirection} (from state first keys to state slots) to
     *        populate
     * @return The result {@link TrackingWritableRowSet}
     */
    final TrackingWritableRowSet applyAddsAndMakeInitialRowSet(
            @NotNull final ObjectArraySource<TrackingWritableRowSet> rowSetSource,
            @NotNull final ObjectArraySource<TrackingWritableRowSet> overflowRowSetSource,
            @NotNull final WritableRowRedirection rowRedirection) {
        final RowSetBuilderRandom resultBuilder = RowSetFactory.builderRandom();
        for (long trackerIndex = 0; trackerIndex < size; ++trackerIndex) {
            final long slotAndFlags = updatedStateSlotAndFlags.getLong(trackerIndex);
            final int slot = (int) (slotAndFlags >> FLAG_SHIFT);
            final RowSetBuilderSequential stateBuilder = builders.get(trackerIndex);
            builders.set(trackerIndex, null);

            final long stateFirstKey;
            // noinspection ConstantConditions
            try (final RowSet stateAddedRowSet = stateBuilder.build()) {
                final TrackingWritableRowSet stateRowSet = slotToRowSet(rowSetSource, overflowRowSetSource, slot);
                stateRowSet.insert(stateAddedRowSet);
                stateRowSet.initializePreviousValue();
                stateFirstKey = stateAddedRowSet.firstRowKey();
            }

            rowRedirection.putVoid(stateFirstKey, slot);
            resultBuilder.addKey(stateFirstKey);
        }
        // NB: We should not need to initialize previous value here, as the result RowSet was computed with no
        // mutations.
        return resultBuilder.build().toTracking();
    }

    /**
     * Apply all accumulated removes to this tracker's updated states.
     *
     * @param rowSetSource The {@link TrackingWritableRowSet} column source for the main table
     * @param overflowRowSetSource The {@link TrackingWritableRowSet} column source for the overflow table
     */
    final void applyRemovesToStates(@NotNull final ObjectArraySource<TrackingWritableRowSet> rowSetSource,
            @NotNull final ObjectArraySource<TrackingWritableRowSet> overflowRowSetSource) {
        for (long trackerIndex = 0; trackerIndex < size; ++trackerIndex) {
            final long slotAndFlags = updatedStateSlotAndFlags.getLong(trackerIndex);
            // Since removes are always done first, we need not check the flags here.
            final int slot = (int) (slotAndFlags >> FLAG_SHIFT);
            final RowSetBuilderSequential builder = builders.get(trackerIndex);
            builders.set(trackerIndex, null);

            // noinspection ConstantConditions
            try (final RowSet stateRemovedRowSet = builder.build()) {
                slotToRowSet(rowSetSource, overflowRowSetSource, slot).remove(stateRemovedRowSet);
            }
        }
    }

    /**
     * Apply a shift to all "current pass" states.
     *
     * @param rowSetSource The {@link TrackingWritableRowSet} column source for the main table
     * @param overflowRowSetSource The {@link TrackingWritableRowSet} column source for the overflow table
     * @param beginRange See {@link RowSetShiftData#applyShift(WritableRowSet, long, long, long)}
     * @param endRange See {@link RowSetShiftData#applyShift(WritableRowSet, long, long, long)}
     * @param shiftDelta See {@link RowSetShiftData#applyShift(WritableRowSet, long, long, long)}
     */
    final void applyShiftToStates(@NotNull final ObjectArraySource<TrackingWritableRowSet> rowSetSource,
            @NotNull final ObjectArraySource<TrackingWritableRowSet> overflowRowSetSource,
            final long beginRange,
            final long endRange,
            final long shiftDelta) {
        for (int currentPositionIndex = 0; currentPositionIndex < currentPassSize; ++currentPositionIndex) {
            final int trackerIndex = currentPassPositions.getInt(currentPositionIndex);
            final long slotAndFlags = updatedStateSlotAndFlags.getLong(trackerIndex);
            // Since the current pass is only states responsive to the current shift, we need not check the flags here.
            final int slot = (int) (slotAndFlags >> FLAG_SHIFT);

            RowSetShiftData.applyShift(slotToRowSet(rowSetSource, overflowRowSetSource, slot), beginRange, endRange,
                    shiftDelta);

            updatedStateSlotAndFlags.set(trackerIndex, slotAndFlags ^ FLAG_STATE_IN_CURRENT_PASS);
        }
        currentPassSize = 0;
    }

    /**
     * Apply all accumulated adds to this tracker's updated states.
     *
     * @param rowSetSource The {@link TrackingWritableRowSet} column source for the main table
     * @param overflowRowSetSource The {@link TrackingWritableRowSet} column source for the overflow table
     */
    final void applyAddsToStates(@NotNull final ObjectArraySource<TrackingWritableRowSet> rowSetSource,
            @NotNull final ObjectArraySource<TrackingWritableRowSet> overflowRowSetSource) {
        for (int currentPositionIndex = 0; currentPositionIndex < currentPassSize; ++currentPositionIndex) {
            final int trackerIndex = currentPassPositions.getInt(currentPositionIndex);
            final long slotAndFlags = updatedStateSlotAndFlags.getLong(trackerIndex);
            // Since the current pass is only states with adds, we need not check the flags here.
            final int slot = (int) (slotAndFlags >> FLAG_SHIFT);

            final RowSetBuilderSequential builder = builders.get(trackerIndex);
            builders.set(trackerIndex, null);

            // noinspection ConstantConditions
            try (final RowSet stateAddedRowSet = builder.build()) {
                slotToRowSet(rowSetSource, overflowRowSetSource, slot).insert(stateAddedRowSet);
            }

            updatedStateSlotAndFlags.set(trackerIndex, slotAndFlags ^ FLAG_STATE_IN_CURRENT_PASS);
        }
        currentPassSize = 0;
    }

    @FunctionalInterface
    interface ModifiedColumnSetProducer {

        ModifiedColumnSet produce(boolean someKeyHasAddsOrRemoves, boolean someKeyHasModifies);
    }

    /**
     * Build an {@link TableUpdateImpl} for this tracker's updated states, and update the result
     * {@link TrackingWritableRowSet} and {@link WritableRowRedirection}.
     *
     * @param rowSetSource The {@link TrackingWritableRowSet} column source for the main table
     * @param overflowRowSetSource The {@link TrackingWritableRowSet} column source for the overflow table
     * @param rowSet The result {@link TrackingWritableRowSet} of visible keys to update
     * @param rowRedirection The result {@link WritableRowRedirection} (from state first keys to state slots) to update
     * @param modifiedColumnSetProducer The {@link ModifiedColumnSetProducer} to use for computing the downstream
     *        {@link ModifiedColumnSet}
     * @return The result {@link TableUpdateImpl}
     */
    final TableUpdate makeUpdateFromStates(@NotNull final ObjectArraySource<TrackingWritableRowSet> rowSetSource,
            @NotNull final ObjectArraySource<TrackingWritableRowSet> overflowRowSetSource,
            @NotNull final WritableRowSet rowSet,
            @NotNull final WritableRowRedirection rowRedirection,
            @NotNull final ModifiedColumnSetProducer modifiedColumnSetProducer) {
        // First pass: Removes are handled on their own, because if the key moved to a new state we may reinsert it
        final RowSetBuilderRandom removedBuilder = RowSetFactory.builderRandom();
        int numStatesWithShifts = 0;
        for (long ti = 0; ti < size; ++ti) {
            final long slotAndFlags = updatedStateSlotAndFlags.getLong(ti);
            final byte flags = (byte) (slotAndFlags & FLAG_MASK);
            final int slot = (int) (slotAndFlags >> FLAG_SHIFT);
            final TrackingRowSet current = slotToRowSet(rowSetSource, overflowRowSetSource, slot);
            final long previousFirstKey = current.firstRowKeyPrev();
            if (previousFirstKey == RowSequence.NULL_ROW_KEY) {
                // Nothing to remove
                continue;
            }
            if (current.isEmpty()) {
                // We must have removed everything
                rowRedirection.removeVoid(previousFirstKey);
                removedBuilder.addKey(previousFirstKey);
                continue;
            }
            final long currentFirstKey = current.firstRowKey();
            if (previousFirstKey != currentFirstKey) {
                // First key changed
                rowRedirection.removeVoid(previousFirstKey);
                if (flags == FLAG_STATE_HAS_SHIFTS) {
                    ++numStatesWithShifts;
                } else {
                    // Not just a shift
                    removedBuilder.addKey(previousFirstKey);
                }
            }
        }

        // Second pass: Everything else
        final RowSetBuilderRandom addedBuilder = RowSetFactory.builderRandom();
        final RowSetBuilderRandom modifiedBuilder = RowSetFactory.builderRandom();
        boolean someKeyHasAddsOrRemoves = false;
        boolean someKeyHasModifies = false;
        final RowSetShiftData shiftData;
        try (final WritableLongChunk<RowKeys> previousShiftedFirstKeys =
                WritableLongChunk.makeWritableChunk(numStatesWithShifts);
                final WritableLongChunk<RowKeys> currentShiftedFirstKeys =
                        WritableLongChunk.makeWritableChunk(numStatesWithShifts)) {
            int shiftChunkPosition = 0;
            for (long ti = 0; ti < size; ++ti) {
                final long slotAndFlags = updatedStateSlotAndFlags.getLong(ti);
                final byte flags = (byte) (slotAndFlags & FLAG_MASK);
                final int slot = (int) (slotAndFlags >> FLAG_SHIFT);
                final TrackingRowSet current = slotToRowSet(rowSetSource, overflowRowSetSource, slot);
                if (current.isEmpty()) {
                    // Removes are already handled
                    continue;
                }
                final long previousFirstKey = current.firstRowKeyPrev();
                final long currentFirstKey = current.firstRowKey();
                if (previousFirstKey == RowSequence.NULL_ROW_KEY) {
                    // We must have added something
                    rowRedirection.putVoid(currentFirstKey, slot);
                    addedBuilder.addKey(currentFirstKey);
                    continue;
                }
                if (previousFirstKey == currentFirstKey) {
                    if (flags != FLAG_STATE_HAS_SHIFTS) {
                        // @formatter:off
                        someKeyHasAddsOrRemoves |= ((flags & (FLAG_STATE_HAS_REMOVES | FLAG_STATE_HAS_ADDS)) != 0);
                        someKeyHasModifies      |= ((flags & FLAG_STATE_HAS_MODIFIES                       ) != 0);
                        // @formatter:on
                        modifiedBuilder.addKey(currentFirstKey);
                    }
                } else {
                    rowRedirection.putVoid(currentFirstKey, slot);
                    if (flags == FLAG_STATE_HAS_SHIFTS) {
                        previousShiftedFirstKeys.set(shiftChunkPosition, previousFirstKey);
                        currentShiftedFirstKeys.set(shiftChunkPosition, currentFirstKey);
                        ++shiftChunkPosition;
                    } else {
                        addedBuilder.addKey(currentFirstKey);
                    }
                }
            }

            // Now sort shifts and build the shift data
            Assert.eq(numStatesWithShifts, "numStatesWithShift", shiftChunkPosition, "shiftedChunkPosition");
            if (numStatesWithShifts > 0) {
                previousShiftedFirstKeys.setSize(numStatesWithShifts);
                currentShiftedFirstKeys.setSize(numStatesWithShifts);
                try (final LongLongTimsortKernel.LongLongSortKernelContext<RowKeys, RowKeys> sortKernelContext =
                        LongLongTimsortKernel.createContext(numStatesWithShifts)) {
                    LongLongTimsortKernel.sort(sortKernelContext, currentShiftedFirstKeys, previousShiftedFirstKeys);
                }
                final RowSetShiftData.Builder shiftBuilder = new RowSetShiftData.Builder();
                for (int si = 0; si < numStatesWithShifts; ++si) {
                    final long previousKey = previousShiftedFirstKeys.get(si);
                    final long currentKey = currentShiftedFirstKeys.get(si);
                    shiftBuilder.shiftRange(previousKey, previousKey, currentKey - previousKey);
                }
                shiftData = shiftBuilder.build();
            } else {
                shiftData = RowSetShiftData.EMPTY;
            }
        }

        // Build the notification indexes
        final RowSet added = addedBuilder.build();
        final RowSet removed = removedBuilder.build();
        final RowSet modified = modifiedBuilder.build();

        // Update the result TrackingWritableRowSet
        rowSet.remove(removed);
        shiftData.apply(rowSet);
        rowSet.insert(added);

        // Build and return the update
        return new TableUpdateImpl(added, removed, modified, shiftData,
                modifiedColumnSetProducer.produce(someKeyHasAddsOrRemoves, someKeyHasModifies));
    }

    private static TrackingWritableRowSet slotToRowSet(
            @NotNull final ObjectArraySource<TrackingWritableRowSet> rowSetSource,
            @NotNull final ObjectArraySource<TrackingWritableRowSet> overflowRowSetSource,
            final int slot) {
        return IncrementalChunkedByAggregationStateManager.isOverflowLocation(slot)
                ? overflowRowSetSource
                        .get(IncrementalChunkedByAggregationStateManager.hashLocationToOverflowLocation(slot))
                : rowSetSource.get(slot);
    }
}
