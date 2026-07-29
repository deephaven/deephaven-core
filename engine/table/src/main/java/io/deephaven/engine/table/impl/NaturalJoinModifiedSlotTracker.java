//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;

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
     * Sequential builders of left row keys to add to or remove from each slot, parallel to modifiedSlots. Only
     * populated for entries carrying the {@link #FLAG_LEFT_REMOVE} or {@link #FLAG_LEFT_ADD} flag. All left removals
     * are processed and cleared (via {@link #forAllLeftRemovals(LeftRowSetConsumer)}) before any left additions are
     * accumulated, so a single source safely serves both purposes.
     */
    private final ObjectArraySource<RowSetBuilderSequential> slotLeftRowSetBuilders =
            new ObjectArraySource<>(RowSetBuilderSequential.class);
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
    public static final int FLAG_MASK = 0x3F;
    public static final byte FLAG_RIGHT_SHIFT = 0x1;
    public static final byte FLAG_RIGHT_MODIFY_PROBE = 0x2;
    public static final byte FLAG_RIGHT_CHANGE = 0x4;
    public static final byte FLAG_RIGHT_ADD = 0x8;
    /** the slot has accumulated left row keys to remove (in {@link #slotLeftRowSetBuilders}) */
    public static final byte FLAG_LEFT_REMOVE = 0x10;
    /** the slot has accumulated left row keys to add (in {@link #slotLeftRowSetBuilders}) */
    public static final byte FLAG_LEFT_ADD = 0x20;

    /**
     * Remove all entries from the tracker.
     */
    void clear() {
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
     * @param originalRightValue if we are the addition of the slot, what the right value was before our modification
     *        (otherwise ignored)
     * @param flags the flags to or into our state
     *
     * @return the cookie for future access
     */
    public long addMain(final long cookie, final int slot, final long originalRightValue, byte flags) {
        if (originalRightValue < 0) {
            flags |= FLAG_RIGHT_ADD;
        }
        if (!isValidCookie(cookie)) {
            return doAddition(slot, originalRightValue, flags);
        } else {
            return updateFlags(cookie, flags);
        }
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
        final long resultCookie;
        final RowSetBuilderSequential builder;
        if (!isValidCookie(cookie)) {
            resultCookie = doAddition(slot, rightValue, FLAG_LEFT_REMOVE);
            builder = RowSetFactory.builderSequential();
            slotLeftRowSetBuilders.set(getPointerFromCookie(resultCookie), builder);
        } else {
            resultCookie = updateFlags(cookie, FLAG_LEFT_REMOVE);
            final long entryPointer = getPointerFromCookie(cookie);
            final RowSetBuilderSequential existing = slotLeftRowSetBuilders.getUnsafe(entryPointer);
            if (existing == null) {
                builder = RowSetFactory.builderSequential();
                slotLeftRowSetBuilders.set(entryPointer, builder);
            } else {
                builder = existing;
            }
        }
        builder.appendKey(removedRowKey);
        return resultCookie;
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
        final long resultCookie;
        final RowSetBuilderSequential builder;
        if (!isValidCookie(cookie)) {
            resultCookie = doAddition(slot, rightValue, FLAG_LEFT_ADD);
            builder = RowSetFactory.builderSequential();
            slotLeftRowSetBuilders.set(getPointerFromCookie(resultCookie), builder);
        } else {
            resultCookie = updateFlags(cookie, FLAG_LEFT_ADD);
            final long entryPointer = getPointerFromCookie(cookie);
            final RowSetBuilderSequential existing = slotLeftRowSetBuilders.getUnsafe(entryPointer);
            if (existing == null) {
                builder = RowSetFactory.builderSequential();
                slotLeftRowSetBuilders.set(entryPointer, builder);
            } else {
                builder = existing;
            }
        }
        builder.appendKey(addedRowKey);
        return resultCookie;
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
                // cleared has no right-side change to propagate. Skipping it is required for correctness (not just
                // efficiency): its saved originalRightValue is the raw slot state, which for a duplicate RHS key is the
                // internal duplicate-location token rather than the resolved RHS row key, so processing it would
                // spuriously report the right columns as modified for every remaining left row on that key.
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
        for (int ii = 0; ii < pointer; ++ii) {
            final long slotAndFlag = modifiedSlots.getLong(ii);
            if ((slotAndFlag & FLAG_LEFT_REMOVE) == 0) {
                continue;
            }
            final int slot = (int) (slotAndFlag >> FLAG_SHIFT);
            final RowSetBuilderSequential builder = slotLeftRowSetBuilders.getUnsafe(ii);
            try (final WritableRowSet removed = builder.build()) {
                consumer.accept(slot, removed);
            }
            slotLeftRowSetBuilders.set(ii, null);
            modifiedSlots.set(ii, slotAndFlag & ~(long) FLAG_LEFT_REMOVE);
        }
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
        for (int ii = 0; ii < pointer; ++ii) {
            final long slotAndFlag = modifiedSlots.getLong(ii);
            if ((slotAndFlag & FLAG_LEFT_ADD) == 0) {
                continue;
            }
            final int slot = (int) (slotAndFlag >> FLAG_SHIFT);
            final RowSetBuilderSequential builder = slotLeftRowSetBuilders.getUnsafe(ii);
            try (final WritableRowSet added = builder.build()) {
                consumer.accept(slot, added);
            }
            slotLeftRowSetBuilders.set(ii, null);
            modifiedSlots.set(ii, slotAndFlag & ~(long) FLAG_LEFT_ADD);
        }
    }

    interface ModifiedSlotConsumer {
        void accept(int slot, long originalRightValue, byte flag);
    }

    public interface LeftRowSetConsumer {
        void accept(int slot, WritableRowSet keys);
    }
}
