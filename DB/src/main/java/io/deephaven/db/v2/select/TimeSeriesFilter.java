
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.select;

import io.deephaven.base.verify.Require;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.live.LiveTable;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.utils.Index;

import java.util.Collections;
import java.util.List;

/**
 * This will filter a table for the most recent N nanoseconds (must be on a date time column).
 */
public class TimeSeriesFilter extends SelectFilterLivenessArtifactImpl implements LiveTable {
    protected final String columnName;
    protected final long nanos;
    private RecomputeListener listener;
    transient private boolean initialized = false;

    @SuppressWarnings("UnusedDeclaration")
    public TimeSeriesFilter(String columnName, String period) {
        this(columnName, DBTimeUtils.expressionToNanos(period));
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

        LiveTableMonitor.DEFAULT.addTable(this);
        initialized = true;
    }

    @Override
    public Index filter(Index selection, Index fullSet, Table table, boolean usePrev) {
        if (usePrev) {
            throw new PreviousFilteringNotSupported();
        }

        @SuppressWarnings("unchecked")
        ColumnSource<DBDateTime> dateColumn = table.getColumnSource(columnName);
        if (!DBDateTime.class.isAssignableFrom(dateColumn.getType())) {
            throw new RuntimeException(columnName + " is not a DBDateTime column!");
        }

        long nanoBoundary = getNow().getNanos() - nanos;

        Index.SequentialBuilder indexBuilder = Index.FACTORY.getSequentialBuilder();
        for (Index.Iterator it = selection.iterator(); it.hasNext();) {
            long row = it.nextLong();
            long nanoValue = dateColumn.get(row).getNanos();
            if (nanoValue >= nanoBoundary) {
                indexBuilder.appendKey(row);
            }
        }

        return indexBuilder.getIndex();
    }

    protected DBDateTime getNow() {
        return DBDateTime.now();
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
    public void refresh() {
        listener.requestRecomputeMatched();
    }

    @Override
    protected void destroy() {
        super.destroy();
        LiveTableMonitor.DEFAULT.removeTable(this);
    }
}
