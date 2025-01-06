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
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter;
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

    private RecomputeListener listener;

    /**
     * For unit tests, we must be able to cause the window check to update.
     */
    private Runnable refreshFunctionForUnitTests;

    /**
     * The listener is responsible for listening to the WindowCheck result, updating our RowSet that contains the rows
     * in our window, and then notifying the WhereListener that we are requesting recomputation.
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
        if (usePrev) {
            throw new PreviousFilteringNotSupported();
        }

        Assert.neqNull(windowListener, "windowListener");

        if (invert) {
            return selection.minus(windowListener.inWindowRowSet);
        } else {
            return selection.intersect(windowListener.inWindowRowSet);
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
        return windowListener.satisfied(step);
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

    private class TimeSeriesFilterWindowListener extends InstrumentedTableUpdateListenerAdapter {
        // the list of rows that exist within our window
        final WritableRowSet inWindowRowSet = RowSetFactory.empty();

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

        windowListener = new TimeSeriesFilterWindowListener(
                "TimeSeriesFilter(" + columnName + ", " + Duration.ofNanos(periodNanos) + ", " + invert + ")",
                tableWithWindow, windowSourceName);
        tableWithWindow.addUpdateListener(windowListener);
        manage(windowListener);

        // we are doing the first match, which is based on the entire set of values in the table
        windowListener.insertMatched(sourceTable.getRowSet());

        // the only thing we hold is our listener, which in turn holds the windowed table
        return null;
    }
}
