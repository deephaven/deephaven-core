//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import it.unimi.dsi.fastutil.longs.LongArrayList;

/**
 * A tracker for modified join hash table slots.
 *
 * After adding an entry, you get back a cookie, which must be passed in on future modification operations for that
 * slot.
 *
 * To process the entries after modifications are complete, call {@link #forAllModifiedSlots(ModifiedSlotConsumer)}.
 */
public class NaturalJoinModifiedSlotTracker {
    private static final int CHUNK_SIZE = 4096;
    private final LongArraySource modifiedSlots = new LongArraySource();
    /** the original right values, parallel to modifiedSlots. */
    private final LongArraySource originalRightValues = new LongArraySource();
    /**
     * Sequential builders of left row keys to remove from, shift within, or add to each slot, parallel to
     * modifiedSlots. Only populated for entries carrying the {@link #FLAG_LEFT_REMOVE}, {@link #FLAG_LEFT_SHIFT} or
     * {@link #FLAG_LEFT_ADD} flag. The left removals are all processed and cleared (via
     * {@link #forAllLeftRemovals(LeftRowSetConsumer)}) before any left shifts are accumulated, and the shifts of each
     * shift range are processed and cleared (via {@link #forAllLeftShifts(LeftRowSetConsumer)}) before the next range
     * or any left additions are accumulated, so a single source safely serves all three purposes.
     */
    private final ObjectArraySource<RowSetBuilderSequential> slotLeftRowSetBuilders =
            new ObjectArraySource<>(RowSetBuilderSequential.class);
    /**
     * /** The entries (as pointers into modifiedSlots) whose builders hold the shifted keys of the shift range being
     * accumulated. Shifts are applied once per shift range, so {@link #forAllLeftShifts(LeftRowSetConsumer)} visits
     * these entries rather than every entry of the cycle; the single removal and addition passes scan all entries.
     */
    private final LongArrayList pendingShiftEntries = new LongArrayList();
    /**
     * the location that we must write to in modified slots; also if we have a pointer that falls outside the range [0,
     * pointer); then we know it is invalid
     */
    private long pointer;
    /** how many slots we have allocated */
    private long allocated;
    /** Each time we clear, we add an offset to our cookies, this prevents us from reading old values */
    private long cookieGeneration;

    private static final int FLAG_SHIFT = 16;
    public static final int FLAG_MASK = 0x7F;
    public static final byte FLAG_RIGHT_SHIFT = 0x1;
    public static final byte FLAG_RIGHT_MODIFY_PROBE = 0x2;
    public static final byte FLAG_RIGHT_CHANGE = 0x4;
    public static final byte FLAG_RIGHT_ADD = 0x8;
    /** the slot has accumulated left row keys to remove (in {@link #slotLeftRowSetBuilders}) */
    public static final byte FLAG_LEFT_REMOVE = 0x10;
    /** the slot has accumulated left row keys to add (in {@link #slotLeftRowSetBuilders}) */
    public static final byte FLAG_LEFT_ADD = 0x20;
    /** the slot has accumulated post-shift left row keys of one shift range (in {@link #slotLeftRowSetBuilders}) */
    public static final byte FLAG_LEFT_SHIFT = 0x40;

    /**
     * Remove all entries from the tracker.
     */
    void clear() {
        pendingShiftEntries.clear();
        cookieGeneration += pointer;
        if (cookieGeneration > Long.MAX_VALUE / 2) {
            cookieGeneration = 0;
        }
        pointer = 0;
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

    /**
     * Add a slot in the main table.
     *
     * @param slot the slot to add.
     * @param originalRightValue the slot's right state before this modification (a right row key, a duplicate location
     *        token, or one of the no-right-row markers); recorded only if this call creates the entry
     * @param flags the flags to or into our state
     *
     * @return the cookie for future access
     */
    public long addMain(final long cookie, final int slot, final long originalRightValue, byte flags) {
        // The no-right-row markers are all negative, but so are the duplicate location tokens, which denote a slot that
        // already had several right rows.
        if (originalRightValue < 0 && !IncrementalNaturalJoinStateManager.isDuplicateRightState(originalRightValue)) {
            flags |= FLAG_RIGHT_ADD;
        }
        if (!isValidCookie(cookie)) {
            return doAddition(slot, originalRightValue, flags);
        } else {
            return updateFlags(cookie, flags);
        }
    }


    /**
     * Record a right row arriving at {@code addedRightRowKey} in {@code slot}. Beyond {@code flags}, this marks
     * {@link #FLAG_RIGHT_ADD} when the arriving row takes the row key this slot's entry recorded as its original right
     * row: the left rows' redirection then names a different row than it did before, even though the row key it holds
     * is unchanged. That happens when the previously selected right row shifts away from its key within the same cycle
     * and a new right row for the same join key is added at the vacated key.
     *
     * @param cookie the slot's existing cookie (or an invalid cookie if this slot has not been tracked yet)
     * @param slot the hash slot (encoding main/alternate via the insert mask)
     * @param originalRightValue the slot's right state before this addition
     * @param addedRightRowKey the row key of the arriving right row
     * @param flags the flags to or into our state
     * @return the cookie for future access
     */
    public long addMainRightAdd(final long cookie, final int slot, final long originalRightValue,
            final long addedRightRowKey, byte flags) {
        final long entryOriginalRightValue = isValidCookie(cookie)
                ? originalRightValues.getLong(getPointerFromCookie(cookie))
                : originalRightValue;
        if (entryOriginalRightValue == addedRightRowKey) {
            flags |= FLAG_RIGHT_ADD;
        }
        return addMain(cookie, slot, originalRightValue, flags);
    }

    private long doAddition(final int slot, final long originalRightValue, byte flags) {
        if (pointer == allocated) {
            allocated += CHUNK_SIZE;
            modifiedSlots.ensureCapacity(allocated);
            originalRightValues.ensureCapacity(allocated);
            slotLeftRowSetBuilders.ensureCapacity(allocated);
        }
        modifiedSlots.set(pointer, ((long) slot << FLAG_SHIFT) | flags);
        originalRightValues.set(pointer, originalRightValue);
        return getCookieFromPointer(pointer++);
    }

    /**
     * Accumulate a left row key that must be removed from {@code slot}. The removal is not performed here; the key is
     * appended to the slot's sequential builder and the {@link #FLAG_LEFT_REMOVE} flag is set. The caller performs the
     * removals in bulk later via {@link #forAllLeftRemovals(LeftRowSetConsumer)}.
     *
     * @param cookie the slot's existing cookie (or an invalid cookie if this slot has not been tracked yet)
     * @param slot the hash slot (encoding main/alternate via the insert mask)
     * @param removedRowKey the left row key to remove from the slot
     * @param rightValue the slot's current right state, used as the original right value if we must allocate an entry
     * @return the cookie for future access
     */
    public long addLeftRemoval(final long cookie, final int slot, final long removedRowKey, final long rightValue) {
        return accumulateLeftRowKey(cookie, slot, removedRowKey, rightValue, FLAG_LEFT_REMOVE, null);
    }

    /**
     * Accumulate a left row key that must be added to {@code slot}. The insertion is not performed here; the key is
     * appended to the slot's sequential builder and the {@link #FLAG_LEFT_ADD} flag is set. The caller performs the
     * insertions in bulk later via {@link #forAllLeftAdditions(LeftRowSetConsumer)}.
     *
     * @param cookie the slot's existing cookie (or an invalid cookie if this slot has not been tracked yet)
     * @param slot the hash slot (encoding main/alternate via the insert mask)
     * @param addedRowKey the left row key to add to the slot
     * @param rightValue the slot's current right state, used as the original right value if we must allocate an entry
     * @return the cookie for future access
     */
    public long addLeftAddition(final long cookie, final int slot, final long addedRowKey, final long rightValue) {
        return accumulateLeftRowKey(cookie, slot, addedRowKey, rightValue, FLAG_LEFT_ADD, null);
    }

    /**
     * Accumulate the post-shift row key of a left row of {@code slot} that one shift range moves. The slot's left row
     * set is not touched here; the key is appended to the slot's sequential builder and the {@link #FLAG_LEFT_SHIFT}
     * flag is set. The caller performs the shift of each slot's rows in bulk via
     * {@link #forAllLeftShifts(LeftRowSetConsumer)} once the range's rows have all been probed.
     *
     * @param cookie the slot's existing cookie (or an invalid cookie if this slot has not been tracked yet)
     * @param slot the hash slot (encoding main/alternate via the insert mask)
     * @param shiftedRowKey the post-shift row key of the left row
     * @param rightValue the slot's current right state, used as the original right value if we must allocate an entry
     * @return the cookie for future access
     */
    public long addLeftShift(final long cookie, final int slot, final long shiftedRowKey, final long rightValue) {
        return accumulateLeftRowKey(cookie, slot, shiftedRowKey, rightValue, FLAG_LEFT_SHIFT, pendingShiftEntries);
    }

    /**
     * Append {@code rowKey} to the sequential builder for {@code slot} and set {@code flag}, allocating the tracker
     * entry and/or the builder if necessary. Shared by {@link #addLeftRemoval}, {@link #addLeftShift} and
     * {@link #addLeftAddition}.
     */
    private long accumulateLeftRowKey(final long cookie, final int slot, final long rowKey, final long rightValue,
            final byte flag, final LongArrayList pendingEntries) {
        final long resultCookie;
        final long entryPointer;
        final RowSetBuilderSequential existing;
        if (!isValidCookie(cookie)) {
            resultCookie = doAddition(slot, rightValue, flag);
            entryPointer = getPointerFromCookie(resultCookie);
            existing = null;
        } else {
            resultCookie = updateFlags(cookie, flag);
            entryPointer = getPointerFromCookie(cookie);
            existing = slotLeftRowSetBuilders.getUnsafe(entryPointer);
        }
        final RowSetBuilderSequential builder;
        if (existing == null) {
            builder = RowSetFactory.builderSequential();
            slotLeftRowSetBuilders.set(entryPointer, builder);
            if (pendingEntries != null) {
                pendingEntries.add(entryPointer);
            }
        } else {
            builder = existing;
        }
        builder.appendKey(rowKey);
        return resultCookie;
    }

    /**
     * Discard the entry for a slot that has been tombstoned. The slot no longer describes the key the entry was
     * recorded for, so the entry must not be applied to whatever key later reuses the slot.
     *
     * @param cookie the slot's cookie; an invalid cookie means the slot has no entry this cycle
     */
    public void removeEntry(final long cookie) {
        if (!isValidCookie(cookie)) {
            return;
        }
        final long pointer = getPointerFromCookie(cookie);
        // left removals are applied (and their builders consumed) before a slot can become empty, and left shifts and
        // additions only accumulate after all removals, so a tombstoned slot never has pending left row keys
        Assert.eqNull(slotLeftRowSetBuilders.getUnsafe(pointer), "slotLeftRowSetBuilders.getUnsafe(pointer)");
        modifiedSlots.set(pointer, modifiedSlots.getLong(pointer) & ~(long) FLAG_MASK);
    }

    private long updateFlags(final long cookie, byte flags) {
        final long pointer = getPointerFromCookie(cookie);
        final long existingValue = modifiedSlots.getLong(pointer);
        modifiedSlots.set(pointer, existingValue | flags);
        return cookie;
    }

    /**
     * For each main and overflow value, call slotConsumer.
     *
     * @param slotConsumer the consumer of our values
     */
    void forAllModifiedSlots(ModifiedSlotConsumer slotConsumer) {
        for (int ii = 0; ii < pointer; ++ii) {
            final long slotAndFlag = modifiedSlots.getLong(ii);
            final byte flag = (byte) (slotAndFlag & FLAG_MASK);
            if (flag == 0) {
                // A pure left add/remove entry whose FLAG_LEFT_ADD/FLAG_LEFT_REMOVE has already been consumed and
                // cleared has no right-side change to propagate.
                continue;
            }
            final int slot = (int) (slotAndFlag >> FLAG_SHIFT);
            slotConsumer.accept(slot, originalRightValues.getLong(ii), flag);
        }
    }

    /**
     * Move a main table location.
     *
     * @param oldTableLocation the old hash slot
     * @param newTableLocation the new hash slot
     */
    public void moveTableLocation(long cookie, @SuppressWarnings("unused") int oldTableLocation,
            int newTableLocation) {
        if (isValidCookie(cookie)) {
            final long pointer = getPointerFromCookie(cookie);
            final long existingSlotAndFlag = modifiedSlots.getLong(pointer);
            final byte flag = (byte) (existingSlotAndFlag & FLAG_MASK);
            final long newSlotAndFlag = ((long) newTableLocation << FLAG_SHIFT) | flag;
            modifiedSlots.set(pointer, newSlotAndFlag);
        }
    }

    /**
     * For each slot that has accumulated left removals, build the removed-key row set and pass it to the consumer, then
     * discard the slot's builder and clear its {@link #FLAG_LEFT_REMOVE} flag (so a subsequent removal pass and the
     * final {@link #forAllModifiedSlots(ModifiedSlotConsumer)} pass do not re-process it). The row set handed to the
     * consumer is owned by this method and closed after the consumer returns.
     *
     * @param consumer the consumer of each slot's removed left row keys
     */
    public void forAllLeftRemovals(LeftRowSetConsumer consumer) {
        forAllLeftSlots(FLAG_LEFT_REMOVE, consumer);
    }

    /**
     * For each slot that has accumulated left shifts, build the post-shift row set of the shifted keys and pass it to
     * the consumer, then discard the slot's builder and clear its {@link #FLAG_LEFT_SHIFT} flag (so a subsequent shift
     * range and the final {@link #forAllModifiedSlots(ModifiedSlotConsumer)} pass do not re-process it). The row set
     * handed to the consumer is owned by this method and closed after the consumer returns.
     *
     * @param consumer the consumer of each slot's shifted left row keys
     */
    public void forAllLeftShifts(LeftRowSetConsumer consumer) {
        final int pendingCount = pendingShiftEntries.size();
        for (int pi = 0; pi < pendingCount; ++pi) {
            final long entryPointer = pendingShiftEntries.getLong(pi);
            final long slotAndFlag = modifiedSlots.getUnsafe(entryPointer);
            Assert.neqZero(slotAndFlag & FLAG_LEFT_SHIFT, "slotAndFlag & FLAG_LEFT_SHIFT");
            consumeLeftEntry(entryPointer, slotAndFlag, FLAG_LEFT_SHIFT, consumer);
        }
        pendingShiftEntries.clear();
    }

    /**
     * For each slot that has accumulated left additions, build the added-key row set and pass it to the consumer, then
     * discard the slot's builder and clear its {@link #FLAG_LEFT_ADD} flag (so the final
     * {@link #forAllModifiedSlots(ModifiedSlotConsumer)} pass does not re-process it). The row set handed to the
     * consumer is owned by this method and closed after the consumer returns.
     *
     * @param consumer the consumer of each slot's added left row keys
     */
    public void forAllLeftAdditions(LeftRowSetConsumer consumer) {
        forAllLeftSlots(FLAG_LEFT_ADD, consumer);
    }

    /**
     * For each slot carrying {@code flag}, build its accumulated left row keys and pass them to the consumer, then
     * discard the slot's builder and clear {@code flag}. Shared by {@link #forAllLeftRemovals} and
     * {@link #forAllLeftAdditions}, each of which runs once per cycle over every entry. The row set handed to the
     * consumer is owned by this method and closed after the consumer returns.
     */
    private void forAllLeftSlots(final byte flag, final LeftRowSetConsumer consumer) {
        for (int ii = 0; ii < pointer; ++ii) {
            final long slotAndFlag = modifiedSlots.getLong(ii);
            if ((slotAndFlag & flag) == 0) {
                continue;
            }
            consumeLeftEntry(ii, slotAndFlag, flag, consumer);
        }
    }

    /**
     * Build the left row keys accumulated for the entry at {@code entryPointer}, whose modifiedSlots value
     * {@code slotAndFlag} carries {@code flag}, pass them to the consumer, then discard the entry's builder and clear
     * {@code flag}.
     */
    private void consumeLeftEntry(final long entryPointer, final long slotAndFlag, final byte flag,
            final LeftRowSetConsumer consumer) {
        final int slot = (int) (slotAndFlag >> FLAG_SHIFT);
        final RowSetBuilderSequential builder = slotLeftRowSetBuilders.getAndSetUnsafe(entryPointer, null);
        try (final WritableRowSet rowKeys = builder.build()) {
            consumer.accept(slot, rowKeys);
        }
        // the consumer may have discarded the entry, so the flags are read back rather than reused
        modifiedSlots.set(entryPointer, modifiedSlots.getUnsafe(entryPointer) & ~(long) flag);
    }

    interface ModifiedSlotConsumer {
        void accept(int slot, long originalRightValue, byte flag);
    }

    public interface LeftRowSetConsumer {
        void accept(int slot, WritableRowSet keys);
    }
}
