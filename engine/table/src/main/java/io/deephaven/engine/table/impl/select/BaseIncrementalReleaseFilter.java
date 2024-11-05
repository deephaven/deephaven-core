//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.ScriptApi;
import org.jetbrains.annotations.NotNull;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.util.Collections;
import java.util.List;

/**
 * Base class for filters that will release more rows of a table on each UGP cycle.
 * <p>
 * The use case is for benchmarks that want to replay a table in order to better understand incremental processing
 * capacity.
 */
public abstract class BaseIncrementalReleaseFilter
        extends WhereFilterLivenessArtifactImpl
        implements Runnable, NotificationQueue.Dependency {

    private final long initialSize;
    private long releasedSize;
    private long expectedSize;

    private RecomputeListener listener;
    private boolean releaseMoreEntries = false;

    transient private boolean addedToUpdateGraph = false;

    private transient volatile long firstReleaseNanos = QueryConstants.NULL_LONG;
    private transient volatile long releaseAllNanos = QueryConstants.NULL_LONG;

    /**
     * Should we release entries during the PeriodicUpdateGraph cycle?
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
    public void init(@NotNull final TableDefinition tableDefinition) {
        initialized = true;
        if (!started) {
            return;
        }
        addToUpdateGraph();
    }

    private void addToUpdateGraph() {
        if (!addedToUpdateGraph) {
            updateGraph.addSource(this);
            addedToUpdateGraph = true;
        }
    }

    @NotNull
    @Override
    public WritableRowSet filter(
            @NotNull final RowSet selection,
            @NotNull final RowSet fullSet,
            @NotNull final Table table,
            final boolean usePrev) {
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
            updateGraph.removeSource(this);
            listener = null;
        }

        return fullSet.subSetByPositionRange(0, releasedSize).intersect(selection);
    }

    /**
     * Callback that is executed when all of our expected rows have been released.
     */
    void onReleaseAll() {
        releaseAllNanos = System.nanoTime();
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
        if (updateGraph.currentThreadProcessesUpdates()) {
            throw new IllegalStateException(
                    "Can not wait for completion while on PeriodicUpdateGraph refresh thread, updates would block.");
        }
        if (releaseAllNanos != QueryConstants.NULL_LONG) {
            return;
        }
        updateGraph.exclusiveLock().doLocked(() -> {
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
        if (updateGraph.currentThreadProcessesUpdates()) {
            throw new IllegalStateException(
                    "Can not wait for completion while on PeriodicUpdateGraph refresh thread, updates would block.");
        }
        if (releaseAllNanos != QueryConstants.NULL_LONG) {
            return;
        }
        final long end = System.currentTimeMillis() + timeoutMillis;
        updateGraph.exclusiveLock().doLocked(() -> {
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
            addToUpdateGraph();
        }
    }

    @Override
    public boolean satisfied(long step) {
        return updateGraph.satisfied(step);
    }

    @Override
    public UpdateGraph getUpdateGraph() {
        return updateGraph;
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
    public BaseIncrementalReleaseFilter copy() {
        throw new UnsupportedOperationException(getClass().getName() + " does not support automatic copy() due to " +
                "usage incompatibilities (internally-created instances cannot be start()ed)");
    }

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
            firstReleaseNanos = System.nanoTime();
        }
        releaseMoreEntries = true;
        listener.requestRecompute();
    }

    @OverridingMethodsMustInvokeSuper
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
