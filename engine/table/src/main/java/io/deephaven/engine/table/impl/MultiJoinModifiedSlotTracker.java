package io.deephaven.engine.table.impl;


import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.table.impl.sources.LongArraySource;

import java.util.ArrayList;
import java.util.List;

public class MultiJoinModifiedSlotTracker {
    /** The slot that was modified. */
    private final IntegerArraySource modifiedSlots = new IntegerArraySource();
    /**
     * We store flags, one per byte; 16 per location in flags, contiguous for multiple tables when we have more than 16.
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
    private int pointer;
    /** how many slots we have allocated */
    private int allocated;
    /** Each time we clear, we add an offset to our cookies, this prevents us from reading old values */
    private int cookieGeneration;

    static final long SENTINEL_UNINITIALIZED_KEY = -2;

    private static final int FLAG_BITS = 4;
    private static final int FLAGS_PER_LOCATION = 16;

    public static final byte FLAG_ADD = 0x1;
    public static final byte FLAG_REMOVE = 0x2;
    public static final byte FLAG_MODIFY = 0x4;
    public static final byte FLAG_SHIFT = 0x8;

    /**
     * Remove all entries from the tracker.
     */
    void clear() {
        cookieGeneration += pointer;
        if (cookieGeneration > Integer.MAX_VALUE / 2) {
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
    private boolean isValidCookie(int cookie) {
        return cookie >= cookieGeneration && getPointerFromCookie(cookie) < pointer;
    }

    /**
     * Get a cookie to return to the user, given a pointer value.
     *
     * @param pointer the pointer to convert to a cookie
     * @return the cookie to return to the user
     */
    private int getCookieFromPointer(int pointer) {
        return cookieGeneration + pointer;
    }

    /**
     * Given a valid user's cookie, return the corresponding pointer.
     *
     * @param cookie the valid cookie
     * @return the pointer into modifiedSlots
     */
    private int getPointerFromCookie(int cookie) {
        return cookie - cookieGeneration;
    }

    /**
     * Add a slot in the main table.
     *
     * @param slot the slot to add to the tracker.
     * @param originalRedirection if we are the addition of the slot, what the right value was before our modification
     *        (otherwise ignored)
     * @param flags the flags to or into our state
     *
     * @return the cookie for future access
     */
    public int addSlot(final int cookie, final int slot, final int tableNumber, final long originalRedirection,
            final byte flags) {
        if (!isValidCookie(cookie)) {
            // Create a new slot in the tracker and reset the flags and redirection.
            maybeAllocateChunk();
            initializeNextTrackerSlot(slot, tableNumber, flags);

            for (int ii = 0; ii < this.originalRedirection.size(); ++ii) {
                final LongArraySource originalRedirectionForTable = this.originalRedirection.get(ii);
                // Store the original redirection for this table, reset the others.
                if (ii == tableNumber) {
                    originalRedirectionForTable.set(pointer, originalRedirection);
                } else {
                    originalRedirectionForTable.set(pointer, SENTINEL_UNINITIALIZED_KEY);
                }
            }
            return getCookieFromPointer(pointer++);
        } else {
            // This tracker slot exists, update the flags and set the redirection for this table.
            final int pointer = getPointerFromCookie(cookie);

            doFlagUpdate(tableNumber, flags, pointer);

            final LongArraySource originalRedirectionForTable = this.originalRedirection.get(tableNumber);
            if (originalRedirectionForTable.getUnsafe(pointer) == SENTINEL_UNINITIALIZED_KEY) {
                originalRedirectionForTable.set(pointer, originalRedirection);
            }
            return cookie;
        }
    }

    /**
     * Modify an existing slot in the main table.
     *
     * @param slot the slot to add to the tracker.
     * @param flags the flags to or into our state
     *
     * @return the cookie for future access
     */
    public int modifySlot(final int cookie, final int slot, final int tableNumber, final byte flags) {
        if (!isValidCookie(cookie)) {
            // Create a new slot in the tracker and reset the flags and redirection for all tables.
            maybeAllocateChunk();
            initializeNextTrackerSlot(slot, tableNumber, flags);
            for (final LongArraySource originalRedirectionForTable : this.originalRedirection) {
                originalRedirectionForTable.set(pointer, SENTINEL_UNINITIALIZED_KEY);
            }
            return getCookieFromPointer(pointer++);
        } else {
            // This tracker slot exists, update the flags only.
            final int pointer = getPointerFromCookie(cookie);
            doFlagUpdate(tableNumber, flags, pointer);
            return cookie;
        }
    }

    private long flagLocationForSlotAndTable(final int pointer, final int tableNumber) {
        return pointer * flagLocationsPerSlot + (tableNumber / FLAGS_PER_LOCATION);
    }

    private int flagShiftForTable(final int tableNumber) {
        return (tableNumber % FLAGS_PER_LOCATION) * FLAG_BITS;
    }

    private long setFlagInLong(final int tableNumber, final byte flag) {
        return ((long) flag) << flagShiftForTable(tableNumber);
    }

    private long setFlagInLong(long existingLong, int tableNumber, byte flag) {
        return existingLong | setFlagInLong(tableNumber, flag);
    }

    private void maybeAllocateChunk() {
        if (pointer == allocated) {
            allocated += JoinControl.CHUNK_SIZE;
            modifiedSlots.ensureCapacity(allocated);
            flagSource.ensureCapacity(allocated * flagLocationsPerSlot);
            this.originalRedirection.forEach(las -> las.ensureCapacity(allocated));
        }
    }

    private void initializeNextTrackerSlot(final int slot, final int tableNumber, final byte flags) {
        modifiedSlots.set(pointer, slot);
        for (int ii = 0; ii < flagLocationsPerSlot; ++ii) {
            flagSource.set(pointer * flagLocationsPerSlot + ii, 0L);
        }
        flagSource.set(flagLocationForSlotAndTable(pointer, tableNumber), setFlagInLong(tableNumber, flags));
    }

    private void doFlagUpdate(int tableNumber, byte flags, int pointer) {
        final long flagLocation = flagLocationForSlotAndTable(pointer, tableNumber);
        final long existingFlagLong = flagSource.getUnsafe(flagLocation);
        final long updatedFlagLong = setFlagInLong(existingFlagLong, tableNumber, flags);
        flagSource.set(flagLocation, updatedFlagLong);
    }

    public interface ModifiedSlotConsumer {
        void accept(int slot, long[] previousRedirections, byte[] flags);
    }

    void forAllModifiedSlots(ModifiedSlotConsumer slotConsumer) {
        final long[] previousRedirections = new long[numTables];
        final byte[] flagValues = new byte[numTables];
        for (long ii = 0; ii < pointer; ++ii) {
            final int slot = modifiedSlots.getInt(ii);

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

            for (tt = 0; tt < previousRedirections.length; ++tt) {
                previousRedirections[tt] = originalRedirection.get(tt).getUnsafe(ii);
            }
            slotConsumer.accept(slot, previousRedirections, flagValues);
        }
    }
}
