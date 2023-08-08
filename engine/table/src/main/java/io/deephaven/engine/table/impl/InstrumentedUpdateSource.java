/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.impl.perf.PerformanceEntry;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph;

import javax.annotation.Nullable;

public abstract class InstrumentedUpdateSource implements Runnable {

    @Nullable
    protected final PerformanceEntry entry;

    public InstrumentedUpdateSource(final UpdateGraph updateGraph, final String description) {
        this.entry = PeriodicUpdateGraph.createUpdatePerformanceEntry(updateGraph, description);
    }

    @Override
    public final void run() {
        if (entry != null) {
            entry.onUpdateStart();
        }
        try {
            instrumentedRefresh();
        } finally {
            if (entry != null) {
                entry.onUpdateEnd();
            }
        }
    }

    protected abstract void instrumentedRefresh();
}
