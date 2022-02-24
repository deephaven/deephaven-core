/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.select;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.ScriptApi;

import java.util.Collections;
import java.util.List;

/**
 * Base class for filters that will release more rows of a table on each UGP cycle.
 *
 * The use case is for benchmarks that want to replay a table in order to better understand incremental processing
 * capacity.
 */
public abstract class BaseIncrementalReleaseFilter extends WhereFilterLivenessArtifactImpl implements Runnable {
    private final long initialSize;
    private long releasedSize;
    private long expectedSize;

    private RecomputeListener listener;
    private boolean releaseMoreEntries = false;

    transient private boolean addedToUpdateGraphProcessor = false;

    private transient volatile long firstReleaseNanos = QueryConstants.NULL_LONG;
    private transient volatile long releaseAllNanos = QueryConstants.NULL_LONG;

    /**
     * Should we release entries during the UpdateGraphProcessor cycle?
     */
    private transient volatile boolean started;
    private transient volatile boolean initialized = false;

    /**
     * Construct an incremental release filter.
     *
     * @param initialSize how many rows should be released in the initialized table before any updates
     * @param started should updates proceed immediately
     */
    BaseIncrementalReleaseFilter(long initialSize, boolean started) {
        releasedSize = this.initialSize = initialSize;
        this.started = started;
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
        initialized = true;
        if (!started) {
            return;
        }
        addToUpdateGraphProcessor();
    }

    private void addToUpdateGraphProcessor() {
        if (!addedToUpdateGraphProcessor) {
            UpdateGraphProcessor.DEFAULT.addSource(this);
            addedToUpdateGraphProcessor = true;
        }
    }

    @Override
    public WritableRowSet filter(RowSet selection, RowSet fullSet, Table table, boolean usePrev) {
        if (usePrev) {
            Assert.eqZero(releasedSize, "releasedSize");
            Assert.eq(fullSet.size(), "fullSet.size()", selection.size(), "selection.size()");
            return fullSet.subSetByPositionRange(0, releasedSize).intersect(selection);
        }

        expectedSize = fullSet.size();

        if (releaseMoreEntries) {
            releasedSize += getSizeIncrement();
        }

        if (fullSet.size() <= releasedSize) {
            onReleaseAll();
            releasedSize = fullSet.size();
            UpdateGraphProcessor.DEFAULT.removeSource(this);
        }

        return fullSet.subSetByPositionRange(0, releasedSize).intersect(selection);
    }

    /**
     * Callback that is executed when all of our expected rows have been released.
     */
    void onReleaseAll() {
        releaseAllNanos = DateTimeUtils.currentTime().getNanos();
        if (firstReleaseNanos == QueryConstants.NULL_LONG) {
            // there was no processing to do
            firstReleaseNanos = releaseAllNanos;
        }
    }

    /**
     * Wait for all rows to be released.
     */
    @ScriptApi
    public void waitForCompletion() throws InterruptedException {
        if (UpdateGraphProcessor.DEFAULT.isRefreshThread()) {
            throw new IllegalStateException(
                    "Can not wait for completion while on UpdateGraphProcessor refresh thread, updates would block.");
        }
        if (releaseAllNanos != QueryConstants.NULL_LONG) {
            return;
        }
        UpdateGraphProcessor.DEFAULT.exclusiveLock().doLocked(() -> {
            while (releaseAllNanos == QueryConstants.NULL_LONG) {
                // this only works because we will never actually filter out a row from the result; in the general
                // WhereFilter case, the result table may not update. We could await on the source table, but
                listener.getTable().awaitUpdate();
            }
        });
    }

    /**
     * Wait for all rows to be released.
     */
    @ScriptApi
    public void waitForCompletion(long timeoutMillis) throws InterruptedException {
        if (UpdateGraphProcessor.DEFAULT.isRefreshThread()) {
            throw new IllegalStateException(
                    "Can not wait for completion while on UpdateGraphProcessor refresh thread, updates would block.");
        }
        if (releaseAllNanos != QueryConstants.NULL_LONG) {
            return;
        }
        final long end = System.currentTimeMillis() + timeoutMillis;
        UpdateGraphProcessor.DEFAULT.exclusiveLock().doLocked(() -> {
            while (releaseAllNanos == QueryConstants.NULL_LONG) {
                // this only works because we will never actually filter out a row from the result; in the general
                // WhereFilter case, the result table may not update. We could await on the source table, but
                final long remainingTimeout = Math.max(0, end - System.currentTimeMillis());
                if (remainingTimeout == 0) {
                    return;
                }
                listener.getTable().awaitUpdate(remainingTimeout);
            }
        });
    }

    /**
     * How many nanos between the first release event and the final release event?
     * 
     * @return nano duration of this filter, or NULL_LONG if the filter is not completed
     */
    @ScriptApi
    public long durationNanos() {
        if (releaseAllNanos == QueryConstants.NULL_LONG || firstReleaseNanos == QueryConstants.NULL_LONG) {
            return QueryConstants.NULL_LONG;
        }
        return releaseAllNanos - firstReleaseNanos;
    }

    /**
     * Begin releasing rows during update propagation.
     */
    public void start() {
        started = true;
        if (initialized) {
            addToUpdateGraphProcessor();
        }
    }

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
    public void run() {
        if (!started) {
            throw new IllegalStateException();
        }
        if (firstReleaseNanos == QueryConstants.NULL_LONG) {
            firstReleaseNanos = DateTimeUtils.currentTime().getNanos();
        }
        releaseMoreEntries = true;
        listener.requestRecompute();
    }

    @Override
    protected void destroy() {
        super.destroy();
        UpdateGraphProcessor.DEFAULT.removeSource(this);
    }
}
