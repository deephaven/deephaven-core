/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.impl.perf.PerformanceEntry;
import io.deephaven.engine.table.impl.perf.UpdatePerformanceTracker;

public abstract class InstrumentedUpdateSource implements Runnable {

    protected final PerformanceEntry entry;

    public InstrumentedUpdateSource(String description) {
        this.entry = UpdatePerformanceTracker.getInstance().getEntry(description);
    }

    @Override
    public final void run() {
        entry.onUpdateStart();
        try {
            instrumentedRefresh();
        } finally {
            entry.onUpdateEnd();
        }
    }

    protected abstract void instrumentedRefresh();
}
