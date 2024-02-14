/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */

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

import java.time.Instant;
import java.util.Collections;
import java.util.List;

/**
 * This will filter a table for the most recent N nanoseconds (must be on a date time column).
 */
public class TimeSeriesFilter
        extends WhereFilterLivenessArtifactImpl
        implements Runnable, NotificationQueue.Dependency {
    protected final String columnName;
    protected final long nanos;
    private RecomputeListener listener;

    @SuppressWarnings("UnusedDeclaration")
    public TimeSeriesFilter(String columnName, String period) {
        this(columnName, DateTimeUtils.parseDurationNanos(period));
    }

    public TimeSeriesFilter(String columnName, long nanos) {
        Require.gtZero(nanos, "nanos");
        this.columnName = columnName;
        this.nanos = nanos;
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
    public void init(TableDefinition tableDefinition) {}

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
        return Clock.system().currentTimeNanos();
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
        return new TimeSeriesFilter(columnName, nanos);
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
