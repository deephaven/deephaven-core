package io.deephaven.db.v2;

import io.deephaven.db.v2.sources.LongArraySource;

/**
 * A tracker for modified join hash table slots.
 *
 * After adding an entry, you get back a cookie, which must be passed in on future modification
 * operations for that slot.
 *
 * To process the entries after modifications are complete, call
 * {@link #forAllModifiedSlots(ModifiedSlotConsumer)}.
 */
class NaturalJoinModifiedSlotTracker {
    private static final int CHUNK_SIZE = 4096;
    private final LongArraySource modifiedSlots = new LongArraySource();
    /** the original right values, parallel to modifiedSlots. */
    private final LongArraySource originalRightValues = new LongArraySource();
    /**
     * the location that we must write to in modified slots; also if we have a pointer that falls
     * outside the range [0, pointer); then we know it is invalid
     */
    private long pointer;
    /** how many slots we have allocated */
    private long allocated;
    /**
     * Each time we clear, we add an offset to our cookies, this prevents us from reading old values
     */
    private long cookieGeneration;

    private static final int FLAG_SHIFT = 16;
    static final int FLAG_MASK = 0xF;
    static final byte FLAG_RIGHT_SHIFT = 0x1;
    static final byte FLAG_RIGHT_MODIFY_PROBE = 0x2;
    static final byte FLAG_RIGHT_CHANGE = 0x4;
    static final byte FLAG_RIGHT_ADD = 0x8;

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
     * Is this cookie within our valid range (greater than or equal to our generation, but less than
     * the pointer after adjustment?
     *
     * @param cookie the cookie to check for validity
     *
     * @return true if the cookie is from the current generation, and references a valid slot in our
     *         table
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
     * @param originalRightValue if we are the addition of the slot, what the right value was before
     *        our modification (otherwise ignored)
     * @param flags the flags to or into our state
     *
     * @return the cookie for future access
     */
    long addMain(final long cookie, final long slot, final long originalRightValue, byte flags) {
        if (originalRightValue < 0) {
            flags |= FLAG_RIGHT_ADD;
        }
        if (!isValidCookie(cookie)) {
            return doAddition(slot, originalRightValue, flags);
        } else {
            return updateFlags(cookie, flags);
        }
    }


    /**
     * Add a slot in the overflow table.
     *
     * @param overflow the slot to add (0...n in the overflow table).
     * @param originalRightValue if we are the addition of the slot, what the right value was before
     *        our modification (otherwise ignored)
     *
     * @return the cookie for future access
     */
    long addOverflow(final long cookie, final long overflow, final long originalRightValue,
        byte flags) {
        final long slot = IncrementalChunkedNaturalJoinStateManager.overflowToSlot(overflow);
        if (originalRightValue < 0) {
            flags |= FLAG_RIGHT_ADD;
        }
        if (!isValidCookie(cookie)) {
            return doAddition(slot, originalRightValue, flags);
        } else {
            return updateFlags(cookie, flags);
        }
    }

    private long doAddition(final long slot, final long originalRightValue, byte flags) {
        if (pointer == allocated) {
            allocated += CHUNK_SIZE;
            modifiedSlots.ensureCapacity(allocated);
            originalRightValues.ensureCapacity(allocated);
        }
        modifiedSlots.set(pointer, (slot << FLAG_SHIFT) | flags);
        originalRightValues.set(pointer, originalRightValue);
        return getCookieFromPointer(pointer++);
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
     * Main values are represented as values >= 0. Overflow values are represented as negative
     * values according to {@link IncrementalChunkedNaturalJoinStateManager#overflowToSlot(long)}.
     *
     * @param slotConsumer the consumer of our values
     */
    void forAllModifiedSlots(ModifiedSlotConsumer slotConsumer) {
        for (int ii = 0; ii < pointer; ++ii) {
            final long slotAndFlag = modifiedSlots.getLong(ii);
            final long slot = slotAndFlag >> FLAG_SHIFT;
            final byte flag = (byte) (slotAndFlag & FLAG_MASK);
            slotConsumer.accept(slot, originalRightValues.getLong(ii), flag);
        }
    }

    /**
     * Move a main table location.
     *
     * @param oldTableLocation the old hash slot
     * @param newTableLocation the new hash slot
     */
    void moveTableLocation(long cookie, @SuppressWarnings("unused") long oldTableLocation,
        long newTableLocation) {
        if (isValidCookie(cookie)) {
            final long pointer = getPointerFromCookie(cookie);
            final long existingSlotAndFlag = modifiedSlots.getLong(pointer);
            final byte flag = (byte) (existingSlotAndFlag & FLAG_MASK);
            final long newSlotAndFlag = (newTableLocation << FLAG_SHIFT) | flag;
            modifiedSlots.set(pointer, newSlotAndFlag);
        }
    }

    /**
     * Move a location from overflow to the main table.
     *
     * @param overflowLocation the old overflow location
     * @param tableLocation the new table location
     */
    void promoteFromOverflow(long cookie, @SuppressWarnings("unused") long overflowLocation,
        long tableLocation) {
        if (isValidCookie(cookie)) {
            final long pointer = getPointerFromCookie(cookie);
            final long existingSlotAndFlag = modifiedSlots.getLong(pointer);
            final byte flag = (byte) (existingSlotAndFlag & FLAG_MASK);
            final long newSlotAndFlag = (tableLocation << FLAG_SHIFT) | flag;
            modifiedSlots.set(pointer, newSlotAndFlag);
        }
    }

    interface ModifiedSlotConsumer {
        void accept(long slot, long originalRightValue, byte flag);
    }
}
