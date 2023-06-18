/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.rowset.RowSet;

import java.util.Collections;
import java.util.List;

/**
 * This will filter a table starting off with the first N rows, and then adding new rows to the table on each run.
 */
public class RollingReleaseFilter extends WhereFilterLivenessArtifactImpl implements Runnable {
    private final long workingSize;
    private final long rollingSize;
    private long offset = 0;

    private RecomputeListener listener;
    private boolean releaseMoreEntries = false;

    public RollingReleaseFilter(long workingSize, long rollingSize) {
        this.workingSize = workingSize;
        this.rollingSize = rollingSize;
    }

    @Override
    public List<String> getColumns() {
        return Collections.emptyList();
    }

    @Override
    public List<String> getColumnArrays() {
        return Collections.emptyList();
    }

    @Override
    public void init(TableDefinition tableDefinition) {}

    @Override
    public WritableRowSet filter(RowSet selection, RowSet fullSet, Table table, boolean usePrev) {
        if (usePrev) {
            throw new PreviousFilteringNotSupported();
        }

        if (releaseMoreEntries) {
            offset = (offset + rollingSize) % fullSet.size();
            releaseMoreEntries = false;
        }

        if (offset + workingSize <= fullSet.size()) {
            final TrackingWritableRowSet sub =
                    fullSet.subSetByPositionRange(offset, offset + workingSize).toTracking();
            sub.retain(selection);
            return sub;
        }

        final WritableRowSet sub = fullSet.copy();
        sub.removeRange(sub.get((offset + workingSize) % fullSet.size()), sub.get(offset) - 1);
        sub.retain(selection);

        return sub;
    }

    @Override
    public boolean isSimpleFilter() {
        /* This doesn't execute any user code, so it should be safe to execute it against untrusted data. */
        return true;
    }

    @Override
    public void setRecomputeListener(RecomputeListener listener) {
        Assert.eqNull(this.listener, "this.listener");
        this.listener = listener;
        listener.setIsRefreshing(true);
        updateGraph.addSource(this);
    }

    @Override
    public RollingReleaseFilter copy() {
        return new RollingReleaseFilter(workingSize, rollingSize);
    }

    @Override
    public boolean isRefreshing() {
        return true;
    }

    @Override
    public void run() {
        releaseMoreEntries = true;
        listener.requestRecompute();
    }

    @Override
    protected void destroy() {
        super.destroy();
        updateGraph.removeSource(this);
    }

    @Override
    public boolean permitParallelization() {
        return false;
    }
}
