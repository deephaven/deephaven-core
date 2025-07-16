//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.util.SafeCloseable;

/**
 * Records the results of a push-down predicate filter operation.
 */
public class PushdownResult implements SafeCloseable {

    // Heuristic cost estimates for different push-down operations to find matching rows.
    // Larger numbers indicate operations that are expected to touch more data or incur higher I/O latency; the values
    // are strictly relative.
    /**
     * Only table/row-group statistics are checked, assuming the metadata is already loaded
     */
    public static final long METADATA_STATS_COST = 10_000L;
    /**
     * Column-level Bloom filter needs to be used
     */
    public static final long BLOOM_FILTER_COST = 20_000L;
    /**
     * Requires querying an in-memory index structure
     */
    public static final long IN_MEMORY_DATA_INDEX_COST = 30_000L;
    /**
     * Requires using binary search on sorted data
     */
    public static final long SORTED_DATA_COST = 40_000L;
    /**
     * Requires reading and querying an external index table
     */
    public static final long DEFERRED_DATA_INDEX_COST = 50_000L;

    /**
     * Rows that match the predicate.
     */
    private final WritableRowSet match;

    /**
     * Rows that might match the predicate but would need to be tested to be certain.
     */
    private final WritableRowSet maybeMatch;

    private PushdownResult(
            final WritableRowSet match,
            final WritableRowSet maybeMatch) {
        this.match = match;
        this.maybeMatch = maybeMatch;
    }

    public static PushdownResult of(
            final WritableRowSet match,
            final WritableRowSet maybeMatch) {
        return new PushdownResult(match, maybeMatch);
    }

    public WritableRowSet match() {
        return match;
    }

    public WritableRowSet maybeMatch() {
        return maybeMatch;
    }

    @Override
    public void close() {
        match.close();
        maybeMatch.close();
    }
}
