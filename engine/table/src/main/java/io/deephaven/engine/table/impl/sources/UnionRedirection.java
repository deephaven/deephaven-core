/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.sources;

import io.deephaven.configuration.Configuration;

import java.io.Serializable;
import java.util.Arrays;

/**
 * This class manages the constituent Tables for a UnionColumnSource, so that we can map from an outer (merged) RowSet
 * into the appropriate segment of a component table.
 */
public class UnionRedirection implements Serializable {
    /**
     * What do we tell users when they try to insert into a full row redirection.
     */
    private static final String INDEX_OVERFLOW_MESSAGE =
            "Failure to insert rowSet into UnionRedirection, TrackingWritableRowSet values exceed long.  If you have several recursive merges, consider rewriting your query to do a single merge of many tables.";

    /**
     * This is the minimum size of an initial allocation of a region.
     */
    public static final long CHUNK_MULTIPLE =
            Configuration.getInstance().getLongWithDefault("UnionRedirection.chunkMultiple", 1 << 16);

    /**
     * How many slots do we allocate for tables (one slot per table).
     */
    private static final int INITIAL_SIZE = 8;

    // cached last position, used to avoid the binary search for the table id, when requesting it for consecutive
    // indices
    private final ThreadLocal<Integer> lastPos = ThreadLocal.withInitial(() -> 0);
    private final ThreadLocal<Integer> prevLastPos = ThreadLocal.withInitial(() -> 0);

    // how many tables have been added to this redirection
    private int size = 0;

    // the start of our outer RowSet for this entry, the end of the current entry (+ 1) is in the next table
    long[] startOfIndices = new long[INITIAL_SIZE];

    // the start of our outer prev RowSet for this entry, the end of the current entry (+ 1) is in the next table
    long[] prevStartOfIndices = new long[INITIAL_SIZE];

    // copy of prevStartOfIndices to be updated during the UGP cycle and swapped as a terminal notification
    long[] prevStartOfIndicesAlt = new long[INITIAL_SIZE];

    /**
     * Fetch the table id for a given row key.
     * 
     * @param index the row key to lookup
     * @return table id of where to find content
     */
    int tidForIndex(long index) {
        int localTid = lastPos.get();
        if (index >= startOfIndices[localTid]) {
            if (index < startOfIndices[localTid + 1]) {
                return localTid;
            }
            localTid = Arrays.binarySearch(startOfIndices, localTid + 1, size, index);
        } else {
            localTid = Arrays.binarySearch(startOfIndices, 0, localTid, index);
        }
        if (localTid < 0) {
            localTid = -localTid - 2;
        }

        lastPos.set(localTid);

        return localTid;
    }

    /**
     * Fetch the table id for a given row key.
     * 
     * @param index the row key to lookup
     * @return table id of where to find content
     */
    int tidForPrevIndex(long index) {
        int localTid = prevLastPos.get();
        if (index >= prevStartOfIndices[localTid]) {
            if (index < prevStartOfIndices[localTid + 1]) {
                return localTid;
            }
            localTid = Arrays.binarySearch(prevStartOfIndices, localTid + 1, size, index);
        } else {
            localTid = Arrays.binarySearch(prevStartOfIndices, 0, localTid, index);
        }
        if (localTid < 0) {
            localTid = -localTid - 2;
        }

        prevLastPos.set(localTid);

        return localTid;
    }

    /**
     * Rounds key up to the nearest boundary with friendly properties.
     * 
     * @param key the max key for a given table
     * @return a multiple of {@code CHUNK_MULTIPLE} higher than provided key
     */
    private long roundToRegionBoundary(long key) {
        long numChunks = key / CHUNK_MULTIPLE + 1;

        if (numChunks < 0) {
            throw new UnsupportedOperationException(INDEX_OVERFLOW_MESSAGE);
        }

        // Require empty tables have non-empty keyspace so that we can binary search on key to find source table.
        return Math.max(1, numChunks) * CHUNK_MULTIPLE;
    }

    /**
     * Append a new table at the end of this union with the given maxKey. It is expected that tables will be added in
     * tableId order.
     *
     * @param maxKey the maximum key of the table
     */
    public void appendTable(long maxKey) {
        if (size + 1 == startOfIndices.length) {
            startOfIndices = Arrays.copyOf(startOfIndices, size * 2);
            prevStartOfIndices = Arrays.copyOf(prevStartOfIndices, size * 2);
            prevStartOfIndicesAlt = Arrays.copyOf(prevStartOfIndicesAlt, size * 2);
        }

        final long keySpace = roundToRegionBoundary(maxKey);

        ++size;
        startOfIndices[size] = startOfIndices[size - 1] + keySpace;
        prevStartOfIndices[size] = prevStartOfIndices[size - 1] + keySpace;
        prevStartOfIndicesAlt[size] = prevStartOfIndicesAlt[size - 1] + keySpace;

        if (startOfIndices[size] < 0 || prevStartOfIndices[size] < 0 || prevStartOfIndicesAlt[size] < 0) {
            throw new UnsupportedOperationException(INDEX_OVERFLOW_MESSAGE);
        }
    }

    /**
     * Computes any shift that should be applied to future tables.
     * 
     * @param tableId the table id of the table that might need more space
     * @param maxKey the max key for the table with tableId
     * @return the relative shift to be applied to all tables with greater tableIds
     */
    public long computeShiftIfNeeded(int tableId, long maxKey) {
        long keySpace = roundToRegionBoundary(maxKey);
        long currRange = startOfIndices[tableId + 1] - startOfIndices[tableId];
        return Math.max(0, keySpace - currRange);
    }
}
