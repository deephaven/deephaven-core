//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.Pair;
import io.deephaven.base.clock.Clock;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter;
import io.deephaven.engine.table.impl.NotificationAwareDependency;
import io.deephaven.engine.table.impl.NotificationStepReceiver;
import io.deephaven.engine.table.impl.QueryTable;
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
 *
 * <p>
 * The filter reads the window column of a table that {@link #beginOperation(Table)} adds the window to, so it can
 * filter previous values as well as current ones. As time passes, rows leave the window, and the filter asks its
 * operation to recompute them. The filter starts following the windowed table only when its operation's snapshot
 * commits, and only if the windowed table has not notified since the snapshot began, so that no row leaving the window
 * is missed.
 * </p>
 */
public class TimeSeriesFilter
        extends WhereFilterLivenessArtifactImpl
        implements NotificationAwareDependency {
    /**
     * A builder to create a TimeSeriesFilter.
     */
    public static class Builder {
        private String columnName;
        private long periodNanos;
        private boolean invert;
        private Clock clock;

        private Builder() {
            // use newBuilder
        }

        /**
         * Set the column name to filter on.
         * 
         * @param columnName the column name to filter on, required
         * @return this builder
         */
        public Builder columnName(final String columnName) {
            this.columnName = columnName;
            return this;
        }

        /**
         * Set an optional Clock to use for this filter. When not specified, the clock is determined by
         * {@link DateTimeUtils#currentClock()}.
         * 
         * @param clock the clock to use for the filter
         * @return this builder
         */
        public Builder clock(final Clock clock) {
            this.clock = clock;
            return this;
        }

        /**
         * Set the period for this filter.
         * 
         * @param period the period as a string for the filter
         * @return this builder
         */
        public Builder period(final String period) {
            return period(DateTimeUtils.parseDurationNanos(period));
        }

        /**
         * Set the period for this filter.
         * 
         * @param period the period as a Duration for the filter
         * @return this builder
         */
        public Builder period(final Duration period) {
            return period(period.toNanos());
        }

        /**
         * Set the period for this filter.
         * 
         * @param period the period in nanoseconds for the filter
         * @return this builder
         */
        public Builder period(final long period) {
            this.periodNanos = period;
            return this;
        }

        /**
         * Set whether this filter should be inverted. An inverted filter accepts only rows that have null timestamps or
         * timestamps older than the period.
         * 
         * @param invert true if the filter should be inverted.
         * @return this builder
         */
        public Builder invert(final boolean invert) {
            this.invert = invert;
            return this;
        }

        /**
         * Create the TimeSeriesFilter described by this builder.
         * 
         * @return a TimeSeriesFilter using the options from this builder.
         */
        public TimeSeriesFilter build() {
            if (columnName == null) {
                throw new IllegalArgumentException("Column name is required");
            }
            return new TimeSeriesFilter(columnName, periodNanos, invert, clock);
        }
    }

    /**
     * Create a builder for a time series filter.
     * 
     * @return a Builder object to configure a TimeSeriesFilter
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    private final String columnName;
    private final long periodNanos;
    private final boolean invert;
    private final Clock clock;
    private Boolean isRefreshing;

    private volatile RecomputeListener listener;

    /**
     * For unit tests, we must be able to cause the window check to update.
     */
    private Runnable refreshFunctionForUnitTests;

    /**
     * The source table with the window column added, created by {@link #beginOperation(Table)}.
     */
    private QueryTable tableWithWindow;

    /**
     * The listener is responsible for listening to the WindowCheck result, and asking the operation to recompute the
     * rows that leave the window.
     */
    private TimeSeriesFilterWindowListener windowListener;

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
        this(columnName, periodNanos, false, null);
    }

    /**
     * Create a TimeSeriesFilter on the given column for the given period in nanoseconds.
     *
     * @param columnName the name of the timestamp column
     * @param periodNanos the duration of the window in nanoseconds
     * @param invert true if only rows outside the window (or with a null timestamp) should be included in the result
     * @param clock the Clock to use as a time source for this filter, when null the clock supplied by
     *        {@link DateTimeUtils#currentClock()} is used.
     */
    private TimeSeriesFilter(final String columnName,
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
        Assert.neqNull(windowListener, "windowListener");

        final WritableRowSet inWindow = windowListener.matchInWindow(selection, usePrev);
        if (!invert) {
            return inWindow;
        }
        try (final WritableRowSet ignored = inWindow) {
            return selection.minus(inWindow);
        }
    }

    @Override
    public boolean isSimpleFilter() {
        /* This doesn't execute any user code, so it should be safe to execute it against untrusted data. */
        return true;
    }

    @Override
    public void setRecomputeListener(RecomputeListener listener) {
        // each snapshot attempt sets the listener for its own result
        this.listener = listener;
        if (isRefreshing()) {
            listener.setIsRefreshing(true);
        }
    }

    @Override
    public boolean satisfied(long step) {
        return windowListener.satisfied(step);
    }

    @Override
    public long lastStateChangeStep() {
        return tableWithWindow.isRefreshing()
                ? tableWithWindow.getLastNotificationStep()
                : NotificationStepReceiver.NULL_NOTIFICATION_STEP;
    }

    @Override
    public boolean subscribe(final long requiredLastStateChangeStep) {
        // a static windowed table never changes, so there is nothing to follow and nothing to refuse
        return !tableWithWindow.isRefreshing()
                || tableWithWindow.addUpdateListener(windowListener, requiredLastStateChangeStep);
    }

    @Override
    public void unsubscribe() {
        if (tableWithWindow.isRefreshing()) {
            tableWithWindow.removeUpdateListener(windowListener);
        }
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
        // Before beginOperation we do not know the source table, so we cannot know; answer conservatively, as the
        // interface requires. A filter over a refreshing source is refreshing, which is the common case anyway.
        return isRefreshing == null || isRefreshing;
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

    private class TimeSeriesFilterWindowListener extends InstrumentedTableUpdateListenerAdapter {
        // the columnset that represents our window source
        private final ModifiedColumnSet windowColumnSet;

        // the column source containing the window; which we match on
        private final ColumnSource<Object> windowColumnSource;

        protected TimeSeriesFilterWindowListener(String listenerDescription, QueryTable tableWithWindow,
                final String windowSourceName) {
            super(listenerDescription, tableWithWindow, false);
            this.windowColumnSet = tableWithWindow.newModifiedColumnSet(windowSourceName);
            this.windowColumnSource = tableWithWindow.getColumnSource(windowSourceName);
        }


        @Override
        public void onUpdate(TableUpdate update) {
            if (!update.modifiedColumnSet().containsAny(windowColumnSet)) {
                return;
            }
            // rows leave the window as time passes, which only this listener sees; the operation sees the source's own
            // added, removed and modified rows
            try (final WritableRowSet inWindow = matchInWindow(update.modified(), false);
                    final WritableRowSet movedOutOfWindow = update.modified().minus(inWindow)) {
                if (movedOutOfWindow.isNonempty()) {
                    listener.requestRecompute(movedOutOfWindow);
                }
            }
        }

        private WritableRowSet matchInWindow(final RowSet rowSet, final boolean usePrev) {
            // The original filter did not include nulls for a regular filter, so we do not include them here either to
            // maintain compatibility. That also means the inverted filter is going to include nulls (as the null is
            // less than the current time using Deephaven long comparisons).
            return windowColumnSource.match(usePrev, MatchOptions.REGULAR, rowSet, Boolean.TRUE);
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
    public SafeCloseable beginOperation(@NotNull final Table sourceTable) {
        if (windowListener != null) {
            throw new IllegalStateException("Inputs already initialized, use copy() instead of re-using a WhereFilter");
        }
        String windowSourceName = "__Window_" + columnName;
        while (sourceTable.hasColumns(windowSourceName)) {
            windowSourceName = "_" + windowSourceName;
        }

        final Pair<Table, WindowCheck.TimeWindowListener> pair = WindowCheck.addTimeWindowInternal(clock,
                (QueryTable) sourceTable, columnName, periodNanos + 1, windowSourceName, true);
        tableWithWindow = (QueryTable) pair.first;

        windowListener = new TimeSeriesFilterWindowListener(
                "TimeSeriesFilter(" + columnName + ", " + Duration.ofNanos(periodNanos) + ", " + invert + ")",
                tableWithWindow, windowSourceName);

        if (tableWithWindow.isRefreshing()) {
            // the listener follows the windowed table once the operation's snapshot commits; see subscribe
            refreshFunctionForUnitTests = pair.second;
            manage(windowListener);
            isRefreshing = true;
        } else {
            refreshFunctionForUnitTests = () -> {
            };
            isRefreshing = false;
        }

        // the only thing we hold is our listener, which in turn holds the windowed table
        return null;
    }
}
