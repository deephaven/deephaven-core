package io.deephaven.engine.table.impl;


import io.deephaven.engine.table.impl.sources.LongArraySource;

import java.util.ArrayList;
import java.util.List;

public class MultiJoinModifiedSlotTracker {
    /** The slot that was modified. */
    private final LongArraySource modifiedSlots = new LongArraySource();
    /**
     * We store flags, one per byte; 8 per location in flags, contiguous for multiple tables when we have more than 8.
     */
    private final LongArraySource flagSource = new LongArraySource();
    /** the original right values, parallel to modifiedSlots. */
    private final List<LongArraySource> originalRedirection = new ArrayList<>();

    int numTables = 0;
    int flagLocationsPerSlot = 0;

    /**
     * the location that we must write to in modified slots; also if we have a pointer that falls outside the range [0,
     * pointer); then we know it is invalid
     */
    private long pointer;
    /** how many slots we have allocated */
    private long allocated;
    /** Each time we clear, we add an offset to our cookies, this prevents us from reading old values */
    private long cookieGeneration;

    static final long SENTINEL_UNINITIALIZED_KEY = -2;

    private static final int FLAG_BITS = 4;
    private static final int FLAGS_PER_LOCATION = 16;

    static final byte FLAG_ADD = 0x1;
    static final byte FLAG_REMOVE = 0x2;
    static final byte FLAG_MODIFY = 0x4;
    static final byte FLAG_SHIFT = 0x8;

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

    void ensureTableCapacity(int numTables) {
        while (originalRedirection.size() < numTables) {
            final LongArraySource las = new LongArraySource();
            las.ensureCapacity(allocated);
            originalRedirection.add(las);
        }
        this.numTables = numTables;
        this.flagLocationsPerSlot = (numTables + FLAGS_PER_LOCATION - 1) / FLAGS_PER_LOCATION;
        this.flagSource.ensureCapacity(flagLocationsPerSlot * allocated);
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
     * @param originalRedirection if we are the addition of the slot, what the right value was before our modification
     *        (otherwise ignored)
     * @param flags the flags to or into our state
     *
     * @return the cookie for future access
     */
    long addSlot(final long cookie, final long slot, final int tableNumber, final long originalRedirection,
            byte flags) {
        if (!isValidCookie(cookie)) {
            return doAddition(slot, tableNumber, originalRedirection, flags);
        } else {
            return updateFlags(cookie, tableNumber, originalRedirection, flags);
        }
    }

    /**
     * Modify a slot in the main table.
     *
     * @param slot the slot to add.
     * @param flags the flags to or into our state
     *
     * @return the cookie for future access
     */
    long modifySlot(final long cookie, final long slot, final int tableNumber, byte flags) {
        if (!isValidCookie(cookie)) {
            return doModify(slot, tableNumber, flags);
        } else {
            return updateFlags(cookie, tableNumber, flags);
        }
    }

    private long flagLocationForSlotAndTable(long pointer, int tableNumber) {
        return pointer * flagLocationsPerSlot + (tableNumber / FLAGS_PER_LOCATION);
    }

    private int flagShiftForTable(int tableNumber) {
        return (tableNumber % FLAGS_PER_LOCATION) * FLAG_BITS;
    }

    private long setFlagInLong(int tableNumber, byte flag) {
        return ((long) flag) << flagShiftForTable(tableNumber);
    }

    private long setFlagInLong(long existingLong, int tableNumber, byte flag) {
        return existingLong | ((long) flag) << flagShiftForTable(tableNumber);
    }

    private void maybeAllocateChunk() {
        if (pointer == allocated) {
            allocated += JoinControl.CHUNK_SIZE;
            modifiedSlots.ensureCapacity(allocated);
            flagSource.ensureCapacity(allocated * flagLocationsPerSlot);
            this.originalRedirection.forEach(las -> las.ensureCapacity(allocated));
        }
    }

    private void initializeNextTrackerSlot(long slot, int tableNumber, byte flags) {
        modifiedSlots.set(pointer, slot);
        for (int ii = 0; ii < flagLocationsPerSlot; ++ii) {
            flagSource.set(pointer * flagLocationsPerSlot + ii, 0L);
        }
        flagSource.set(flagLocationForSlotAndTable(pointer, tableNumber), setFlagInLong(tableNumber, flags));
    }

    private long doAddition(final long slot, final int tableNumber, final long originalRedirection, byte flags) {
        maybeAllocateChunk();
        initializeNextTrackerSlot(slot, tableNumber, flags);

        for (int ii = 0; ii < this.originalRedirection.size(); ++ii) {
            final LongArraySource originalRedirectionForTable = this.originalRedirection.get(ii);
            if (ii == tableNumber) {
                originalRedirectionForTable.set(pointer, originalRedirection);
            } else {
                originalRedirectionForTable.set(pointer, SENTINEL_UNINITIALIZED_KEY);
            }
        }
        return getCookieFromPointer(pointer++);
    }


    private long doModify(final long slot, final int tableNumber, byte flags) {
        maybeAllocateChunk();
        initializeNextTrackerSlot(slot, tableNumber, flags);
        for (int ii = 0; ii < this.originalRedirection.size(); ++ii) {
            final LongArraySource originalRedirectionForTable = this.originalRedirection.get(ii);
            originalRedirectionForTable.set(pointer, SENTINEL_UNINITIALIZED_KEY);
        }
        return getCookieFromPointer(pointer++);
    }

    private long updateFlags(final long cookie, final int tableNumber, final long originalRedirection, byte flags) {
        final long pointer = getPointerFromCookie(cookie);

        doFlagUpdate(tableNumber, flags, pointer);

        final LongArraySource originalRedirectionForTable = this.originalRedirection.get(tableNumber);
        if (originalRedirectionForTable.getUnsafe(pointer) == SENTINEL_UNINITIALIZED_KEY) {
            originalRedirectionForTable.set(pointer, originalRedirection);
        }
        return cookie;
    }

    private long updateFlags(final long cookie, final int tableNumber, byte flags) {
        final long pointer = getPointerFromCookie(cookie);
        doFlagUpdate(tableNumber, flags, pointer);
        return cookie;
    }

    private void doFlagUpdate(int tableNumber, byte flags, long pointer) {
        final long flagLocation = flagLocationForSlotAndTable(pointer, tableNumber);
        final long existingFlagLong = flagSource.getUnsafe(flagLocation);
        final long updatedFlagLong = setFlagInLong(existingFlagLong, tableNumber, flags);
        flagSource.set(flagLocation, updatedFlagLong);
    }

    public interface ModifiedSlotConsumer {
        void accept(long slot, long[] originalValues, byte[] flags);
    }

    void forAllModifiedSlots(ModifiedSlotConsumer slotConsumer) {
        final long[] originalValues = new long[numTables];
        final byte[] flagValues = new byte[numTables];
        for (long ii = 0; ii < pointer; ++ii) {
            final long slot = modifiedSlots.getLong(ii);

            int tt = 0;
            for (int ff = 0; ff < flagLocationsPerSlot - 1; ++ff) {
                final long flagLong = flagSource.getUnsafe((ii * flagLocationsPerSlot) + ff);
                for (int jj = 0; jj < FLAGS_PER_LOCATION; ++jj) {
                    flagValues[tt++] = (byte) ((flagLong >> (FLAG_BITS * jj)) & ((1L << FLAG_BITS) - 1));
                }
            }
            final long flagLong = flagSource.getUnsafe((ii * flagLocationsPerSlot) + flagLocationsPerSlot - 1);
            for (int jj = 0; tt < numTables; ++jj) {
                flagValues[tt++] = (byte) ((flagLong >> (FLAG_BITS * jj)) & ((1L << FLAG_BITS) - 1));
            }

            for (tt = 0; tt < originalValues.length; ++tt) {
                originalValues[tt] = originalRedirection.get(tt).getUnsafe(ii);
            }
            slotConsumer.accept(slot, originalValues, flagValues);
        }
    }
}
