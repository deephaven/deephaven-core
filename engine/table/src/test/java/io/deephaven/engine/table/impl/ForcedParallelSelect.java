//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.util.SafeCloseable;

/**
 * Forces every {@code select} or {@code update} built while this is open to compute its parallelizable columns through
 * the job scheduler, both initially and on updates, regardless of whether parallel select and update are enabled or the
 * update graph's parallelism. Restores the previous settings on close.
 * <p>
 * The force flag is a package-private static on {@link QueryTable}, which is why this lives in this package.
 */
public final class ForcedParallelSelect implements SafeCloseable {

    private final boolean previousForce;
    private final long previousMinimumRows;

    /**
     * Force parallel select and update, leaving the minimum rows per job unchanged.
     */
    public ForcedParallelSelect() {
        this(QueryTable.MINIMUM_PARALLEL_SELECT_ROWS);
    }

    /**
     * Force parallel select and update, with the specified minimum rows per job.
     *
     * @param minimumParallelSelectRows The minimum number of rows for each job computing a column; a column with fewer
     *        added and modified rows than this is not divided across jobs
     */
    public ForcedParallelSelect(final long minimumParallelSelectRows) {
        previousForce = QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE;
        previousMinimumRows = QueryTable.MINIMUM_PARALLEL_SELECT_ROWS;
        QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE = true;
        QueryTable.MINIMUM_PARALLEL_SELECT_ROWS = minimumParallelSelectRows;
    }

    @Override
    public void close() {
        QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE = previousForce;
        QueryTable.MINIMUM_PARALLEL_SELECT_ROWS = previousMinimumRows;
    }
}
