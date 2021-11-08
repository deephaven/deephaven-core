/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2;

import io.deephaven.engine.v2.utils.UpdatePerformanceTracker;

public abstract class InstrumentedUpdateSource implements Runnable {

    protected final UpdatePerformanceTracker.Entry entry;

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
