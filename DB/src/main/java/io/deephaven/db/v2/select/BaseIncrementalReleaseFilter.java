/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.select;

import io.deephaven.base.verify.Assert;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.live.LiveTable;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.v2.utils.Index;

import java.util.Collections;
import java.util.List;

/**
 * Base class for filters that will release more rows of a table on each LTM cycle.
 *
 * The use case is for benchmarks that want to replay a table in order to better understand incremental processing
 * capacity.
 */
public abstract class BaseIncrementalReleaseFilter extends SelectFilterLivenessArtifactImpl implements LiveTable {
    private final long initialSize;
    private long releasedSize;
    private long expectedSize;

    private RecomputeListener listener;
    private boolean releaseMoreEntries = false;

    transient private boolean addedToLiveTableMonitor = false;

    BaseIncrementalReleaseFilter(long initialSize) {
        releasedSize = this.initialSize = initialSize;
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
            Assert.eqZero(releasedSize, "releasedSize");
            Assert.eq(fullSet.size(), "fullSet.size()", selection.size(), "selection.size()");
            return fullSet.subindexByPos(0, releasedSize).intersect(selection);
        }

        expectedSize = fullSet.size();

        if (releaseMoreEntries) {
            releasedSize += getSizeIncrement();
        }

        if (fullSet.size() <= releasedSize) {
            onReleaseAll();
            releasedSize = fullSet.size();
        }

        return fullSet.subindexByPos(0, releasedSize).intersect(selection);
    }

    void onReleaseAll() {}

    public long getInitialSize() {
        return initialSize;
    }

    long getReleasedSize() {
        return releasedSize;
    }

    public long getExpectedSize() {
        return expectedSize;
    }

    abstract long getSizeIncrement();

    @Override
    public boolean isSimpleFilter() {
        /* This doesn't execute any user code, so it should be safe to execute it against untrusted data. */
        return true;
    }

    @Override
    public void setRecomputeListener(RecomputeListener listener) {
        this.listener = listener;
        listener.setIsRefreshing(true);
    }

    @Override
    abstract public BaseIncrementalReleaseFilter copy();

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
