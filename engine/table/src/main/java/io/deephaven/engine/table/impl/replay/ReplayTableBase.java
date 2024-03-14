//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.replay;

import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateSource;
import io.deephaven.engine.table.impl.QueryTable;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

public abstract class ReplayTableBase extends QueryTable implements Runnable {

    private final SourceRefresher sourceRefresher;

    public ReplayTableBase(
            @NotNull final String description,
            @NotNull final TrackingRowSet rowSet,
            @NotNull final Map<String, ? extends ColumnSource<?>> columns) {
        super(rowSet, columns);
        setRefreshing(true);
        sourceRefresher = new SourceRefresher(description);
    }

    public void start() {
        updateGraph.addSource(sourceRefresher);
    }

    public void stop() {
        updateGraph.removeSource(sourceRefresher);
    }

    private class SourceRefresher extends InstrumentedTableUpdateSource {
        SourceRefresher(final String description) {
            super(ReplayTableBase.this, description);
        }

        @Override
        protected void instrumentedRefresh() {
            ReplayTableBase.this.run();
        }
    }
}
