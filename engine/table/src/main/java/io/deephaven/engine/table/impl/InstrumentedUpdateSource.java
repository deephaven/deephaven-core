/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.impl.perf.PerformanceEntry;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph;

import javax.annotation.Nullable;

public abstract class InstrumentedUpdateSource implements Runnable {

    protected final UpdateGraph updateGraph;
    @Nullable
    protected final PerformanceEntry entry;

    public InstrumentedUpdateSource(final UpdateGraph updateGraph, final String description) {
        this.updateGraph = updateGraph;
        this.entry = PeriodicUpdateGraph.createUpdatePerformanceEntry(updateGraph, description);
    }

    @Override
    public final void run() {
        if (entry != null) {
            entry.onUpdateStart();
        }
        try {
            instrumentedRefresh();
        } catch (final Exception error) {
            updateGraph.removeSource(this);
            onRefreshError(error);
        } finally {
            if (entry != null) {
                entry.onUpdateEnd();
            }
        }
    }

    protected abstract void instrumentedRefresh();

    protected abstract void onRefreshError(Exception error);
}
