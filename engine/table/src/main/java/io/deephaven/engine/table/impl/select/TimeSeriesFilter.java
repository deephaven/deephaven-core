//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.clock.Clock;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

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
        implements Runnable, NotificationQueue.Dependency {
    private final String columnName;
    private final long nanos;
    private final Clock clock;

    private RecomputeListener listener;

    @SuppressWarnings("UnusedDeclaration")
    public TimeSeriesFilter(final String columnName,
                            final String period) {
        this(columnName, DateTimeUtils.parseDurationNanos(period));
    }

    // TODO: invert
    public TimeSeriesFilter(final String columnName,
                            final long nanos) {
        this(columnName, nanos, null);
    }

    public TimeSeriesFilter(final String columnName,
                            final long periodNanos,
                            @Nullable final Clock clock) {
        Require.gtZero(periodNanos, "periodNanos");
        this.columnName = columnName;
        this.nanos = periodNanos;
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

        // TODO: reinterpret this appropriately, or maybe delegate to the window checking stuff
        ColumnSource<Instant> dateColumn = table.getColumnSource(columnName);
        if (!Instant.class.isAssignableFrom(dateColumn.getType())) {
            throw new RuntimeException(columnName + " is not an Instant column!");
        }

        long nanoBoundary = getNowNanos() - nanos;

        RowSetBuilderSequential indexBuilder = RowSetFactory.builderSequential();
        for (RowSet.Iterator it = selection.iterator(); it.hasNext();) {
            long row = it.nextLong();
            Instant instant = dateColumn.get(row);
            long nanoValue = DateTimeUtils.epochNanos(instant);
            if (nanoValue >= nanoBoundary) {
                indexBuilder.appendKey(row);
            }
        }

        return indexBuilder.build();
    }

    protected long getNowNanos() {
        return Objects.requireNonNullElseGet(clock, DateTimeUtils::currentClock).currentTimeNanos();
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
    public boolean satisfied(long step) {
        return updateGraph.satisfied(step);
    }

    @Override
    public UpdateGraph getUpdateGraph() {
        return updateGraph;
    }

    @Override
    public TimeSeriesFilter copy() {
        return new TimeSeriesFilter(columnName, nanos, clock);
    }

    @Override
    public boolean isRefreshing() {
        return true;
    }

    @Override
    public void run() {
        listener.requestRecomputeMatched();
    }

    @Override
    protected void destroy() {
        super.destroy();
        updateGraph.removeSource(this);
    }
}
