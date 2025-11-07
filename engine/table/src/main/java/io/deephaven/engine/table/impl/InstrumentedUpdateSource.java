//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.impl.perf.PerformanceEntry;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.util.Objects;

public abstract class InstrumentedUpdateSource implements Runnable {

    protected final UpdateSourceRegistrar updateSourceRegistrar;
    @Nullable
    protected final PerformanceEntry entry;

    public InstrumentedUpdateSource(
            @NotNull final UpdateSourceRegistrar updateSourceRegistrar,
            @Nullable final String description) {
        this.updateSourceRegistrar = Objects.requireNonNull(updateSourceRegistrar);
        this.entry = PeriodicUpdateGraph.createUpdatePerformanceEntry(
                updateSourceRegistrar.getUpdateGraph(), description, null);
    }

    @Override
    public final void run() {
        if (entry != null) {
            entry.onUpdateStart();
        }
        try {
            instrumentedRefresh();
        } catch (final Exception error) {
            updateSourceRegistrar.removeSource(this);
            onRefreshError(error);
        } finally {
            if (entry != null) {
                entry.onUpdateEnd();
            }
        }
    }

    protected abstract void instrumentedRefresh();

    protected abstract void onRefreshError(Exception error);

    @Nullable
    public PerformanceEntry getEntry() {
        return entry;
    }
}
