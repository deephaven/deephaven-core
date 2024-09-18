//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.Pair;
import io.deephaven.base.clock.Clock;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.ListenerRecorder;
import io.deephaven.engine.table.impl.MergedListener;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.util.WindowCheck;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.annotations.TestUseOnly;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * This will filter a table for the most recent N nanoseconds (must be on an {@link Instant} column).
 *
 * <p>
 * Note, this filter rescans the source table. You should prefer to use {@link io.deephaven.engine.util.WindowCheck}
 * instead.
 * </p>
 */
public class TimeSeriesFilter
        extends WhereFilterLivenessArtifactImpl
        implements NotificationQueue.Dependency {
    private final String columnName;
    private final long periodNanos;
    private final boolean invert;
    private final Clock clock;

    private RecomputeListener listener;
    // this table contains our window
    private QueryTable tableWithWindow;
    private String windowSourceName;
    private Runnable refreshFunctionForUnitTests;

    /**
     * We create the merged listener so that we can participate in dependency resolution, but the dependencies are not
     * actually known until the first call to filter().
     */
    private final List<ListenerRecorder> windowListenerRecorder = new CopyOnWriteArrayList<>();
    private final List<NotificationQueue.Dependency> windowDependency = new CopyOnWriteArrayList<>();
    private final TimeSeriesFilterMergedListener mergedListener;

    @SuppressWarnings("UnusedDeclaration")
    public TimeSeriesFilter(final String columnName,
            final String period) {
        this(columnName, DateTimeUtils.parseDurationNanos(period));
    }

    public TimeSeriesFilter(final String columnName,
            final long nanos) {
        this(columnName, nanos, false);
    }

    public TimeSeriesFilter(final String columnName,
            final long nanos,
            final boolean invert) {
        this(columnName, nanos, invert, null);
    }

    public TimeSeriesFilter(final String columnName,
            final long periodNanos,
            final boolean invert,
            @Nullable final Clock clock) {
        Require.gtZero(periodNanos, "periodNanos");
        this.columnName = columnName;
        this.periodNanos = periodNanos;
        this.invert = invert;
        this.clock = clock;
        this.mergedListener = new TimeSeriesFilterMergedListener(
                "TimeSeriesFilter(" + columnName + ", " + Duration.ofNanos(periodNanos) + ", " + invert + ")");
    }

    @Override
    public List<String> getColumns() {
        return Collections.singletonList(columnName);
    }

    @Override
    public List<String> getColumnArrays() {
        return Collections.emptyList();
    }

    @Override
    public void init(@NotNull final TableDefinition tableDefinition) {}

    @NotNull
    @Override
    public synchronized WritableRowSet filter(
            @NotNull final RowSet selection,
            @NotNull final RowSet fullSet,
            @NotNull final Table table,
            final boolean usePrev) {
        if (usePrev) {
            throw new PreviousFilteringNotSupported();
        }

        if (tableWithWindow == null) {
            windowSourceName = "__Window_" + columnName;
            while (table.getDefinition().getColumnNames().contains(windowSourceName)) {
                windowSourceName = "_" + windowSourceName;
            }
            final Pair<Table, WindowCheck.TimeWindowListener> pair = WindowCheck.addTimeWindowInternal(clock, (QueryTable) table, columnName, periodNanos + 1, windowSourceName, true);
            tableWithWindow = (QueryTable)pair.first;
            refreshFunctionForUnitTests = pair.second;

            manage(tableWithWindow);
            windowDependency.add(tableWithWindow);
            final ListenerRecorder recorder = new ListenerRecorder("TimeSeriesFilter-ListenerRecorder", tableWithWindow, null);
            tableWithWindow.addUpdateListener(recorder);
            recorder.setMergedListener(mergedListener);

            windowListenerRecorder.add(recorder);

            // we are doing the first match, which is based on the entire set of values in the table
            mergedListener.insertMatched(fullSet);
        }

        return selection.intersect(mergedListener.inWindowRowset);
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
    }

    @Override
    public boolean satisfied(long step) {
        return updateGraph.satisfied(step);
    }

    @Override
    public UpdateGraph getUpdateGraph() {
        return updateGraph;
    }

    @Override
    public TimeSeriesFilter copy() {
        return new TimeSeriesFilter(columnName, periodNanos, invert, clock);
    }

    @Override
    public boolean isRefreshing() {
        return true;
    }

    @Override
    public boolean permitParallelization() {
        return false;
    }

    private class TimeSeriesFilterMergedListener extends MergedListener {
        final WritableRowSet inWindowRowset = RowSetFactory.empty();

        protected TimeSeriesFilterMergedListener(String listenerDescription) {
            super(windowListenerRecorder, windowDependency, listenerDescription, null);
        }

        @Override
        protected void process() {
            synchronized (TimeSeriesFilter.this) {
                final TableUpdate update = windowListenerRecorder.get(0).getUpdate().acquire();

                inWindowRowset.remove(update.removed());
                if (update.modifiedColumnSet().containsAll(tableWithWindow.newModifiedColumnSet(windowSourceName))) {
                    // we need to check on the modified rows; they may be in the window,
                    inWindowRowset.remove(update.getModifiedPreShift());
                    insertMatched(update.modified());
                }
                insertMatched(update.added());

                if (invert) {
                    listener.requestRecomputeUnmatched();
                } else {
                    listener.requestRecomputeMatched();
                }
            }
        }

        private void insertMatched(final RowSet rowSet) {
            try (final RowSet matched = tableWithWindow.getColumnSource(windowSourceName).match(false, false, false, null, rowSet, Boolean.TRUE)) {
                inWindowRowset.insert(matched);
            }
        }
    }

    @TestUseOnly
    void runForUnitTests() {
        refreshFunctionForUnitTests.run();
    }
}
