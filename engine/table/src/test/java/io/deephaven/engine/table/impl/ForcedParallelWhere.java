//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.util.SafeCloseable;

/**
 * Forces every {@code where} listener built while this is open to run its filters through the update graph's job
 * scheduler, in two segments, so that a listener's filter work completes on notifications of its own rather than inside
 * the listener's notification. Restores the previous settings on close.
 * <p>
 * The flags are package-private statics on {@link QueryTable}, which is why this lives in this package.
 */
public final class ForcedParallelWhere implements SafeCloseable {

    private final boolean previousForce;
    private final boolean previousDisable;
    private final int previousSegments;

    public ForcedParallelWhere() {
        previousForce = QueryTable.FORCE_PARALLEL_WHERE;
        previousDisable = QueryTable.DISABLE_PARALLEL_WHERE;
        previousSegments = QueryTable.PARALLEL_WHERE_SEGMENTS;
        QueryTable.FORCE_PARALLEL_WHERE = true;
        QueryTable.DISABLE_PARALLEL_WHERE = false;
        QueryTable.PARALLEL_WHERE_SEGMENTS = 2;
    }

    @Override
    public void close() {
        QueryTable.FORCE_PARALLEL_WHERE = previousForce;
        QueryTable.DISABLE_PARALLEL_WHERE = previousDisable;
        QueryTable.PARALLEL_WHERE_SEGMENTS = previousSegments;
    }
}
