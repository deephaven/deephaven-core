/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.select;

/**
 * This will filter a table starting off with the first N rows, and then adding new rows to the
 * table on each refresh.
 */
public class IncrementalReleaseFilter extends BaseIncrementalReleaseFilter {
    private final long sizeIncrement;

    /**
     * Create an incremental release filter with an initial size that will release sizeIncrement
     * rows each cycle.
     *
     * @param initialSize how many rows should be in the table initially
     * @param sizeIncrement how many rows to release at the beginning of each LTM cycle.
     */
    public IncrementalReleaseFilter(long initialSize, long sizeIncrement) {
        super(initialSize);
        this.sizeIncrement = sizeIncrement;
    }

    @Override
    long getSizeIncrement() {
        return sizeIncrement;
    }

    @Override
    public IncrementalReleaseFilter copy() {
        return new IncrementalReleaseFilter(getInitialSize(), sizeIncrement);
    }
}
