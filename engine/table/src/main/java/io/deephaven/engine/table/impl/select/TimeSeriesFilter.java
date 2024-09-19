//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.Pair;
import io.deephaven.base.clock.Clock;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.ListenerRecorder;
import io.deephaven.engine.table.impl.MergedListener;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.util.WindowCheck;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.TestUseOnly;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

/**
 * TimeSeriesFilter filters a timestamp colum within the table for recent rows.
 *
 * <p>
 * The filtered column must be an Instant or long containing nanoseconds since the epoch. The computation of recency is
 * delegated to {@link io.deephaven.engine.util.WindowCheck}.
 * </p>
 *
 * <p>
 * When the filter is not inverted, null rows are not accepted and rows that match the window exactly are accepted.
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

    /**
     * For unit tests, we must be able to cause the window check to update.
     */
    private Runnable refreshFunctionForUnitTests;

    /**
     * The merged listener is responsible for listening to the WindowCheck result, updating our rowset that contains the
     * rows in our window, and then notifying the WhereListener that we are requesting recomputation.
     */
    private TimeSeriesFilterMergedListener mergedListener;

    /**
     * Create a TimeSeriesFilter on the given column for the given period
     *
     * @param columnName the name of the timestamp column
     * @param period the duration of the window as parsed by {@link DateTimeUtils#parseDurationNanos(String)}.
     */
    @SuppressWarnings("UnusedDeclaration")
    public TimeSeriesFilter(final String columnName,
            final String period) {
        this(columnName, DateTimeUtils.parseDurationNanos(period));
    }

    /**
     * Create a TimeSeriesFilter on the given column for the given period in nanoseconds.
     *
     * @param columnName the name of the timestamp column
     * @param periodNanos the duration of the window in nanoseconds
     */
    public TimeSeriesFilter(final String columnName,
            final long periodNanos) {
        this(columnName, periodNanos, false);
    }

    // TODO: USE A BUILDER FOR THE CONSTRUCTOR

    /**
     * Create a TimeSeriesFilter on the given column for the given period in nanoseconds.
     *
     * <p>
     * The filter may be <i>inverted</i>, meaning that instead of including recent rows within the window it only
     * includes rows outside the window or that are null.
     * </p>
     *
     * @param columnName the name of the timestamp column
     * @param periodNanos the duration of the window in nanoseconds
     * @param invert true if only rows outside the window should be included in the result
     */
    public TimeSeriesFilter(final String columnName,
            final long periodNanos,
            final boolean invert) {
        this(columnName, periodNanos, invert, null);
    }

    /**
     * Create a TimeSeriesFilter on the given column for the given period in nanoseconds.
     *
     * <p>
     * The filter may be <i>inverted</i>, meaning that instead of including recent rows within the window it only
     * includes rows outside the window.
     * </p>
     *
     * @param columnName the name of the timestamp column
     * @param period the duration of the window as parsed by {@link DateTimeUtils#parseDurationNanos(String)}.
     * @param invert true if only rows outside the window should be included in the result
     */
    public TimeSeriesFilter(final String columnName,
            final String period,
            final boolean invert) {
        this(columnName, DateTimeUtils.parseDurationNanos(period), invert, null);
    }

    /**
     * Create a TimeSeriesFilter on the given column for the given period in nanoseconds.
     *
     * <p>
     * The filter may be <i>inverted</i>, meaning that instead of including recent rows within the window it only
     * includes rows outside the window.
     * </p>
     *
     * @param columnName the name of the timestamp column
     * @param periodNanos the duration of the window in nanoseconds
     * @param invert true if only rows outside the window should be included in the result
     * @param clock the Clock to use as a time source for this filter, when null the clock supplied by
     *        {@link DateTimeUtils#currentClock()} is used.
     */
    public TimeSeriesFilter(final String columnName,
            final long periodNanos,
            final boolean invert,
            @Nullable final Clock clock) {
        this.columnName = columnName;
        this.periodNanos = periodNanos;
        this.invert = invert;
        this.clock = clock;
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
    public WritableRowSet filter(
            @NotNull final RowSet selection,
            @NotNull final RowSet fullSet,
            @NotNull final Table table,
            final boolean usePrev) {
        if (usePrev) {
            throw new PreviousFilteringNotSupported();
        }

        Assert.neqNull(mergedListener, "mergedListener");

        if (invert) {
            return selection.minus(mergedListener.inWindowRowSet);
        } else {
            return selection.intersect(mergedListener.inWindowRowSet);
        }
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
        return mergedListener.satisfied(step);
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
        // there is no reason to parallelize this filter, because the actual filtering is only a simple rowset operation
        // and parallelization would cost more than actually applying the rowset operation once
        return false;
    }

    @Override
    public String toString() {
        return "TimeSeriesFilter{" +
                "columnName='" + columnName + '\'' +
                ", periodNanos=" + periodNanos +
                ", invert=" + invert +
                '}';
    }

    // TODO: we should be listener not a merged listener
    private class TimeSeriesFilterMergedListener extends MergedListener {
        // the list of rows that exist within our window
        final WritableRowSet inWindowRowSet = RowSetFactory.empty();

        // this is our listener recorder for tableWithWindow
        private final ListenerRecorder windowRecorder;

        // the columnset that represents our window source
        private final ModifiedColumnSet windowColumnSet;

        // the column source containing the window; which we match on
        private final ColumnSource<Object> windowColumnSource;

        protected TimeSeriesFilterMergedListener(String listenerDescription, QueryTable tableWithWindow,
                final ListenerRecorder windowRecorder, final String windowSourceName) {
            super(Collections.singleton(windowRecorder), Collections.singleton(tableWithWindow), listenerDescription,
                    null);
            this.windowRecorder = windowRecorder;
            this.windowColumnSet = tableWithWindow.newModifiedColumnSet(windowSourceName);
            this.windowColumnSource = tableWithWindow.getColumnSource(windowSourceName);
        }

        @Override
        protected void process() {
            Assert.assertion(windowRecorder.recordedVariablesAreValid(),
                    "windowRecorder.recordedVariablesAreValid()");
            final TableUpdate update = windowRecorder.getUpdate();

            inWindowRowSet.remove(update.removed());
            final boolean windowModified = update.modifiedColumnSet().containsAny(windowColumnSet);
            if (windowModified) {
                // we need to check on the modified rows; they may be in the window,
                inWindowRowSet.remove(update.getModifiedPreShift());
            }
            update.shifted().apply(inWindowRowSet);
            if (windowModified) {
                final RowSet newlyMatched = insertMatched(update.modified());
                try (final WritableRowSet movedOutOfWindow = update.modified().minus(newlyMatched)) {
                    if (movedOutOfWindow.isNonempty()) {
                        listener.requestRecompute(movedOutOfWindow);
                    }
                }
            }
            insertMatched(update.added());
        }

        private RowSet insertMatched(final RowSet rowSet) {
            // The original filter did not include nulls for a regular filter, so we do not include them here either to
            // maintain compatibility. That also means the inverted filter is going to include nulls (as the null is
            // less than the current time using Deephaven long comparisons).
            final RowSet matched = windowColumnSource.match(false, false, false, null, rowSet, Boolean.TRUE);
            inWindowRowSet.insert(matched);
            return matched;
        }
    }

    /**
     * For test uses, causes the WindowCheck to update rows based on the current value of clock.
     */
    @TestUseOnly
    void runForUnitTests() {
        refreshFunctionForUnitTests.run();
    }

    @Override
    public SafeCloseable beginOperation(@NotNull Table sourceTable) {
        String windowSourceName = "__Window_" + columnName;
        while (sourceTable.hasColumns(windowSourceName)) {
            windowSourceName = "_" + windowSourceName;
        }

        final Pair<Table, WindowCheck.TimeWindowListener> pair = WindowCheck.addTimeWindowInternal(clock,
                (QueryTable) sourceTable, columnName, periodNanos + 1, windowSourceName, true);
        final QueryTable tableWithWindow = (QueryTable) pair.first;
        refreshFunctionForUnitTests = pair.second;

        final ListenerRecorder recorder =
                new ListenerRecorder("TimeSeriesFilter-ListenerRecorder", tableWithWindow, null);
        tableWithWindow.addUpdateListener(recorder);

        mergedListener = new TimeSeriesFilterMergedListener(
                "TimeSeriesFilter(" + columnName + ", " + Duration.ofNanos(periodNanos) + ", " + invert + ")",
                tableWithWindow, recorder, windowSourceName);
        manage(mergedListener);

        recorder.setMergedListener(mergedListener);

        // we are doing the first match, which is based on the entire set of values in the table
        mergedListener.insertMatched(sourceTable.getRowSet());

        // the only thing we hold is our mergedListener, which in turn holds the recorder and the windowed table
        return null;
    }
}
