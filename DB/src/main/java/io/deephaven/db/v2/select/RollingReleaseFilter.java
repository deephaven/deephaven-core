/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.select;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.live.LiveTable;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.v2.utils.Index;

import java.util.Collections;
import java.util.List;

/**
 * This will filter a table starting off with the first N rows, and then adding new rows to the
 * table on each refresh.
 */
public class RollingReleaseFilter extends SelectFilterLivenessArtifactImpl implements LiveTable {
    private final long workingSize;
    private final long rollingSize;
    private long offset = 0;

    private RecomputeListener listener;
    private boolean releaseMoreEntries = false;

    transient private boolean addedToLiveTableMonitor = false;

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
    public void init(TableDefinition tableDefinition) {
        if (!addedToLiveTableMonitor) {
            LiveTableMonitor.DEFAULT.addTable(this);
            addedToLiveTableMonitor = true;
        }
    }

    @Override
    public Index filter(Index selection, Index fullSet, Table table, boolean usePrev) {
        if (usePrev) {
            throw new PreviousFilteringNotSupported();
        }

        if (releaseMoreEntries) {
            offset = (offset + rollingSize) % fullSet.size();
            releaseMoreEntries = false;
        }

        if (offset + workingSize <= fullSet.size()) {
            final Index sub = fullSet.subindexByPos(offset, offset + workingSize);
            sub.retain(selection);
            selection.close();
            return sub;
        }

        final Index sub = fullSet.clone();
        sub.removeRange(sub.get((offset + workingSize) % fullSet.size()), sub.get(offset) - 1);
        sub.retain(selection);
        selection.close();

        return sub;
    }

    @Override
    public boolean isSimpleFilter() {
        /*
         * This doesn't execute any user code, so it should be safe to execute it against untrusted
         * data.
         */
        return true;
    }

    @Override
    public void setRecomputeListener(RecomputeListener listener) {
        this.listener = listener;
        listener.setIsRefreshing(true);
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
    public void refresh() {
        releaseMoreEntries = true;
        listener.requestRecompute();
    }

    @Override
    protected void destroy() {
        super.destroy();
        LiveTableMonitor.DEFAULT.removeTable(this);
    }
}
