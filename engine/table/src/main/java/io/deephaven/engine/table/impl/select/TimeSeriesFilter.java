
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.select;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.time.DateTime;
import io.deephaven.engine.table.ColumnSource;

import java.util.Collections;
import java.util.List;

/**
 * This will filter a table for the most recent N nanoseconds (must be on a date time column).
 */
public class TimeSeriesFilter extends WhereFilterLivenessArtifactImpl implements Runnable {
    protected final String columnName;
    protected final long nanos;
    private RecomputeListener listener;
    transient private boolean initialized = false;

    @SuppressWarnings("UnusedDeclaration")
    public TimeSeriesFilter(String columnName, String period) {
        this(columnName, DateTimeUtils.expressionToNanos(period));
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
    public void init(TableDefinition tableDefinition) {
        if (initialized) {
            return;
        }

        UpdateGraphProcessor.DEFAULT.addSource(this);
        initialized = true;
    }

    @Override
    public WritableRowSet filter(RowSet selection, RowSet fullSet, Table table, boolean usePrev) {
        if (usePrev) {
            throw new PreviousFilteringNotSupported();
        }

        @SuppressWarnings("unchecked")
        ColumnSource<DateTime> dateColumn = table.getColumnSource(columnName);
        if (!DateTime.class.isAssignableFrom(dateColumn.getType())) {
            throw new RuntimeException(columnName + " is not a DateTime column!");
        }

        long nanoBoundary = getNow().getNanos() - nanos;

        RowSetBuilderSequential indexBuilder = RowSetFactory.builderSequential();
        for (RowSet.Iterator it = selection.iterator(); it.hasNext();) {
            long row = it.nextLong();
            long nanoValue = dateColumn.get(row).getNanos();
            if (nanoValue >= nanoBoundary) {
                indexBuilder.appendKey(row);
            }
        }

        return indexBuilder.build();
    }

    protected DateTime getNow() {
        return DateTime.now();
    }

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
        UpdateGraphProcessor.DEFAULT.removeSource(this);
    }
}
