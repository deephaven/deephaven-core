/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.sources;

import io.deephaven.base.MathUtil;
import io.deephaven.configuration.Configuration;
import io.deephaven.util.annotations.VisibleForTesting;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.Arrays;

/**
 * This class manages the row key space allocated to constituent Tables for a UnionColumnSource, so that we can map row
 * keys from an outer (merged) RowSet to the enclosing constituent Table.
 */
@VisibleForTesting
public class UnionRedirection implements Serializable {

    /**
     * Message for users when they try to insert into a full row redirection.
     */
    private static final String ROW_SET_OVERFLOW_MESSAGE =
            "Failure to insert row set into UnionRedirection, row keys exceed max long.  If you have several recursive"
                    + " merges, consider rewriting your query to do a single merge of many tables.";

    /**
     * Each constituent is allocated row key space in multiples of this unit.
     */
    @VisibleForTesting
    public static final long ALLOCATION_UNIT_ROW_KEYS =
            Configuration.getInstance().getLongWithDefault("UnionRedirection.allocationUnit", 1 << 16);

    /**
     * Number of table slots to allocate initially.
     */
    private static final int INITIAL_SIZE = 8;

    /**
     * Cached prior slot used by {@link #currSlotForRowKey(long)}.
     */
    private final ThreadLocal<Integer> priorCurrSlot = ThreadLocal.withInitial(() -> 0);

    /**
     * Cached prior slot used by {@link #prevSlotForRowKey(long)}.
     */
    private final ThreadLocal<Integer> priorPrevSlot = ThreadLocal.withInitial(() -> 0);

    /**
     * Number of slots in use in the current version of the union. Note that {@code currFirstRowKeys[currSize]} is the
     * size of the row key space currently allocated to our output.
     */
    private int currSize = 0;

    /**
     * Number of slots in use in the previous version of the union. Note that {@code prevFirstRowKeys[prevSize]} is the
     * size of the row key space previously allocated to our output.
     */
    private int prevSize = 0;

    /**
     * The current first row key for each slot in of our outer RowSet for this entry, the end of the current entry (+ 1)
     * is in the next table
     * 
     */
    private long[] currFirstRowKeys = new long[INITIAL_SIZE];

    // the start of our outer prev RowSet for this entry, the end of the current entry (+ 1) is in the next table
    private long[] prevFirstRowKeys = new long[INITIAL_SIZE];


    /**
     * Get the first row key currently allocated to {@code slot}. This value may be used to "un-shift" the downstream
     * row key space allocated to this slot into the row key space of the upstream table currently occupying this slot.
     *
     * @param slot The slot to lookup
     * @return The first row key currently allocated to the slot
     */
    long currFirstRowKeyForSlot(final int slot) {
        return currFirstRowKeys[slot];
    }

    /**
     * Get the last row key currently allocated to {@code slot}. This value may be used to slice row sequences
     * into the range allocated to the upstream table currently occupying this slot.
     *
     * @param slot The slot to lookup
     * @return The last row key currently allocated to the slot
     */
    long currLastRowKeyForSlot(final int slot){
        return currFirstRowKeys[slot + 1] - 1;
    }

    /**
     * Get the first row key previously allocated to {@code slot}. This value may be used to "un-shift" the downstream
     * row key space allocated to this slot into the row key space of the upstream table previously occupying this slot.
     *
     * @param slot The slot to lookup
     * @return The first row key previously allocated to the slot
     */
    long prevFirstRowKeyForSlot(final int slot) {
        return prevFirstRowKeys[slot];
    }

    /**
     * Get the last row key previously allocated to {@code slot}. This value may be used to slice row sequences
     * into the range allocated to the upstream table previously occupying this slot.
     *
     * @param slot The slot to lookup
     * @return The last row key previously allocated to the slot
     */
    long prevLastRowKeyForSlot(final int slot){
        return prevFirstRowKeys[slot + 1] - 1;
    }

    /**
     * Find the current slot holding {@code rowKey}.
     *
     * @param rowKey The row key to lookup
     * @return Table slot that currently contains the row key
     */
    int currSlotForRowKey(final long rowKey) {
        return slotForRowKey(rowKey, priorCurrSlot, currFirstRowKeys, currSize);
    }

    /**
     * Find the current slot at or after {@code firstSlot} holding {@code rowKey}.
     *
     * @param rowKey The row key to lookup
     * @param firstSlot The first slot to search from, must be {@code >= 0}
     * @return Table slot that currently contains the row key
     */
    int currSlotForRowKey(final long rowKey, final int firstSlot) {
        return slotForRowKey(rowKey, firstSlot, currFirstRowKeys, currSize);
    }

    /**
     * Find the previous slot holding {@code rowKey}.
     *
     * @param rowKey The row key to lookup
     * @return Table slot that previously contained the row key
     */
    int prevSlotForRowKey(final long rowKey) {
        return slotForRowKey(rowKey, priorPrevSlot, prevFirstRowKeys, prevSize);
    }

    /**
     * Find the previous slot at or after {@code firstSlot} holding {@code rowKey}.
     *
     * @param rowKey The row key to lookup
     * @param firstSlot The first slot to search from, must be {@code >= 0}
     * @return Table slot that previously contained the row key
     */
    int prevSlotForRowKey(final long rowKey, final int firstSlot) {
        return slotForRowKey(rowKey, firstSlot, prevFirstRowKeys, prevSize);
    }

    private static int slotForRowKey(final long rowKey, @NotNull final ThreadLocal<Integer> priorSlot,
            @NotNull final long[] firstRowKeyForSlot, final int numSlots) {
        final int firstSlot = priorSlot.get();
        final int slot = slotForRowKey(rowKey, firstSlot, firstRowKeyForSlot, numSlots);
        if (firstSlot != slot) {
            priorSlot.set(slot);
        }
        return slot;
    }

    private static int slotForRowKey(final long rowKey, int firstSlot,
            @NotNull final long[] firstRowKeyForSlot, final int numSlots) {
        if (rowKey >= firstRowKeyForSlot[firstSlot]) {
            if (rowKey < firstRowKeyForSlot[firstSlot + 1]) {
                return firstSlot;
            }
        } else {
            firstSlot = 0;
        }
        final int slot = Arrays.binarySearch(firstRowKeyForSlot, firstSlot, numSlots, rowKey);
        return slot < 0 ? ~slot - 1 : slot;
    }

    /**
     * Compute the key space size appropriate to hold {@code lastRowKey}.
     * 
     * @param lastRowKey The highest row key for a given constituent table
     * @return The key space size to allocate
     */
    private static long keySpaceFor(final long lastRowKey) {
        final long numUnits = lastRowKey / ALLOCATION_UNIT_ROW_KEYS + 1;

        if (numUnits < 0) {
            throw new UnsupportedOperationException(ROW_SET_OVERFLOW_MESSAGE);
        }

        // Require empty tables to have non-empty key space allocation so that we can binary search using a row key to
        // find its source table slot.
        return Math.max(1, numUnits) * ALLOCATION_UNIT_ROW_KEYS;
    }

    /**
     * Update previous redirections to match current redirections. This should be done at the end of initialization, and
     * the end of the updating phase of each UGP cycle.
     */
    void copyCurrToPrev() {
        if (prevFirstRowKeys.length != currFirstRowKeys.length) {
            prevFirstRowKeys = new long[currFirstRowKeys.length];
        }
        System.arraycopy(currFirstRowKeys, 0, prevFirstRowKeys, 0, currFirstRowKeys.length);
        prevSize = currSize;
    }

    void ensureCapacity(final int numTables) {
        if (currFirstRowKeys.length < numTables) {
            currFirstRowKeys = Arrays.copyOf(currFirstRowKeys, 1 << MathUtil.ceilLog2(numTables));
        }
    }

    /**
     * Append a new table at the end of this union with the given maxKey. It is expected that tables will be added in
     * tableId order.
     *
     * @param maxKey the maximum key of the table
     */
    public void appendTable(long maxKey) {
        if (currSize + 1 == currFirstRowKeys.length) {
            currFirstRowKeys = Arrays.copyOf(currFirstRowKeys, currSize * 2);
            prevFirstRowKeys = Arrays.copyOf(prevFirstRowKeys, currSize * 2);
            prevStartOfIndicesAlt = Arrays.copyOf(prevStartOfIndicesAlt, currSize * 2);
        }

        final long keySpace = keySpaceFor(maxKey);

        ++currSize;
        currFirstRowKeys[currSize] = currFirstRowKeys[currSize - 1] + keySpace;
        prevFirstRowKeys[currSize] = prevFirstRowKeys[currSize - 1] + keySpace;
        prevStartOfIndicesAlt[currSize] = prevStartOfIndicesAlt[currSize - 1] + keySpace;

        if (currFirstRowKeys[currSize] < 0 || prevFirstRowKeys[currSize] < 0 || prevStartOfIndicesAlt[currSize] < 0) {
            throw new UnsupportedOperationException(ROW_SET_OVERFLOW_MESSAGE);
        }
    }

    /**
     * Computes any shift that should be applied to tables at higher slots.
     * 
     * @param slot The slot of the table that might need more space
     * @param lastRowKey The last row key in the table at {@code slot}
     * @return The relative shift to be applied to all tables at higher slots
     */
    long computeShiftIfNeeded(final int slot, final long lastRowKey) {
        final long keySpaceNeeded = keySpaceFor(lastRowKey);
        final long keySpaceAllocated = currFirstRowKeys[slot + 1] - currFirstRowKeys[slot];
        return Math.max(0, keySpaceNeeded - keySpaceAllocated);
    }
}
