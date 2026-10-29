//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.util.SafeCloseable;

/**
 * Forces every {@code select} or {@code update} built while this is open to compute its parallelizable columns through
 * the job scheduler, both initially and on updates, regardless of whether parallel select and update are enabled or the
 * update graph's parallelism. Restores the previous setting on close.
 * <p>
 * The flag is a package-private static on {@link QueryTable}, which is why this lives in this package.
 */
public final class ForcedParallelSelect implements SafeCloseable {

    private final boolean previousForce;

    public ForcedParallelSelect() {
        previousForce = QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE;
        QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE = true;
    }

    @Override
    public void close() {
        QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE = previousForce;
    }
}
