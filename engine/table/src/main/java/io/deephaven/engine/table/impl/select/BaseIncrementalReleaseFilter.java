//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.impl.BaseTable;
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
    private boolean releaseMoreRows = false;

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

    /**
     * If this filter is not part of the update graph's sources, then add it.
     */
    protected void addToUpdateGraph() {
        if (addedToUpdateGraph) {
            return;
        }
        updateGraph.addSource(this);
        addedToUpdateGraph = true;
    }

    /**
     * If this filter is part of the update graph's sources, then remove it.
     * <p>
     * Additionally, if the table we are filtering is not refreshing; we drop the reference to our recompute listener.
     * The reference to the recompute listener is dropped, because there are never going to be any more rows to add to
     * the result table.
     * </p>
     */
    protected void removeFromUpdateGraph(@NotNull Table table) {
        if (!addedToUpdateGraph) {
            return;
        }
        updateGraph.removeSource(this);
        addedToUpdateGraph = false;
        if (!table.isRefreshing()) {
            listener = null;
        }
    }

    @NotNull
    @Override
    public WritableRowSet filter(
            @NotNull final RowSet selection,
            @NotNull final RowSet fullSet,
            @NotNull final Table table,
            final boolean usePrev) {
        if (table.isRefreshing()) {
            // we cannot handle anything but append-only tables, because we do not know the complete index of rows
            // that have been released, we only track the count (which must be a prefix of the table
            Assert.assertion(((BaseTable<?>) table).isAppendOnly(), "table.isAppendOnly()");
        }
        if (usePrev) {
            // the selection could be an older version of fullset, which means it must be a prefix for things to work
            // out correctly in the end
            try (final RowSet.RangeIterator sit = selection.rangeIterator();
                    final RowSet.RangeIterator fit = fullSet.rangeIterator()) {
                while (sit.hasNext()) {
                    if (!fit.hasNext()) {
                        throw new IllegalStateException("selection is not a prefix of fullSet");
                    }
                    sit.next();
                    fit.next();
                    if (sit.currentRangeStart() != fit.currentRangeStart()) {
                        throw new IllegalStateException("selection is not a prefix of fullSet");
                    }
                    if (sit.currentRangeEnd() != fit.currentRangeEnd()) {
                        if (sit.hasNext()) {
                            throw new IllegalStateException("selection is not a prefix of fullSet");
                        }
                    }
                }
            }
            return selection.subSetByPositionRange(0, releasedSize);
        }

        setExpectedSize(fullSet.size());

        if (getReleaseMoreRows()) {
            incrementReleasedSize(getSizeIncrement());
        }

        if (fullSet.size() <= releasedSize) {
            setReleasedSize(fullSet.size());
            onReleaseAll();
            removeFromUpdateGraph(table);
        } else {
            // add it back for a refreshing table that has not been completely released
            addToUpdateGraph();
        }

        return fullSet.subSetByPositionRange(0, releasedSize).intersect(selection);
    }

    /**
     * Callback that is executed when all of our expected rows have been released.
     *
     * <p>
     * Note that for a refreshing table, this may be executed more than once.
     * </p>
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
        waitForCompletion(Long.MAX_VALUE, false);
    }

    /**
     * Wait for all rows to be released.
     */
    @ScriptApi
    public void waitForCompletion(long timeoutMillis) throws InterruptedException {
        waitForCompletion(timeoutMillis, true);
    }

    private void waitForCompletion(long timeoutMillis, final boolean hasTimeout) throws InterruptedException {
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
                // This only works because we will never actually filter out a row from the result; in the general
                // WhereFilter case, the result table may not update if not all rows are passed through
                if (hasTimeout) {
                    final long remainingTimeout = Math.max(0, end - System.currentTimeMillis());
                    if (remainingTimeout == 0) {
                        return;
                    }
                    listener.getTable().awaitUpdate(remainingTimeout);
                } else {
                    listener.getTable().awaitUpdate();
                }
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

    /**
     * @return the number of rows released
     */
    long getReleasedSize() {
        return releasedSize;
    }

    /**
     * @return true if {@link #filter(RowSet, RowSet, Table, boolean)} should release more entries
     */
    boolean getReleaseMoreRows() {
        return releaseMoreRows;
    }

    /**
     * @return the expected size of the table, or QueryConstants.NULL_LONG if the table is not known
     */
    public long getExpectedSize() {
        return expectedSize;
    }

    /**
     * Set the expected size of the table.
     *
     * @param expectedSize the expected size of the table
     */
    void setExpectedSize(long expectedSize) {
        this.expectedSize = expectedSize;
    }

    /**
     * Increment the released size by the specified amount.
     */
    void incrementReleasedSize(long increment) {
        releasedSize += increment;
    }

    /**
     * Set the released size.
     *
     * @param releasedSize the size that has been released
     */
    void setReleasedSize(long releasedSize) {
        this.releasedSize = releasedSize;
    }

    /**
     * @return the number of rows to release at this step
     */
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
        releaseMoreRows = true;
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
