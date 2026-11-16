//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.util.SafeCloseable;

/**
 * Forces every {@code where} built while this is open to run its filters through the job scheduler, divided into the
 * requested number of segments, so that a listener's filter work completes on notifications of its own rather than
 * inside the listener's notification. Restores the previous settings on close.
 * <p>
 * The flags are package-private statics on {@link QueryTable}, which is why this lives in this package.
 */
public final class ForcedParallelWhere implements SafeCloseable {

    private final boolean previousForce;
    private final boolean previousDisable;
    private final int previousSegments;
    private final long previousRowsPerSegment;

    /**
     * Force parallel where in two segments, leaving the rows per segment unchanged.
     */
    public ForcedParallelWhere() {
        this(2, QueryTable.PARALLEL_WHERE_ROWS_PER_SEGMENT);
    }

    /**
     * Force parallel where with the specified segmentation.
     *
     * @param segments The number of segments to divide filter work into
     * @param rowsPerSegment The number of rows for each segment of filter work; an input with fewer than
     *        {@code segments * rowsPerSegment} rows is divided into fewer segments
     */
    public ForcedParallelWhere(final int segments, final long rowsPerSegment) {
        previousForce = QueryTable.FORCE_PARALLEL_WHERE;
        previousDisable = QueryTable.DISABLE_PARALLEL_WHERE;
        previousSegments = QueryTable.PARALLEL_WHERE_SEGMENTS;
        previousRowsPerSegment = QueryTable.PARALLEL_WHERE_ROWS_PER_SEGMENT;
        QueryTable.FORCE_PARALLEL_WHERE = true;
        QueryTable.DISABLE_PARALLEL_WHERE = false;
        QueryTable.PARALLEL_WHERE_SEGMENTS = segments;
        QueryTable.PARALLEL_WHERE_ROWS_PER_SEGMENT = rowsPerSegment;
    }

    @Override
    public void close() {
        QueryTable.FORCE_PARALLEL_WHERE = previousForce;
        QueryTable.DISABLE_PARALLEL_WHERE = previousDisable;
        QueryTable.PARALLEL_WHERE_SEGMENTS = previousSegments;
        QueryTable.PARALLEL_WHERE_ROWS_PER_SEGMENT = previousRowsPerSegment;
    }
}
