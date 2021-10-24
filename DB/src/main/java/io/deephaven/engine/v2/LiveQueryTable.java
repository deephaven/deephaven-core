/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2;

import io.deephaven.engine.tables.live.LiveTable;
import io.deephaven.engine.tables.live.LiveTableMonitor;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.utils.RowSetBuilder;
import io.deephaven.engine.v2.utils.TrackingMutableRowSet;

import java.util.Map;

public class LiveQueryTable extends QueryTable implements LiveTable {
    private RowSetBuilder additionsBuilder = TrackingMutableRowSet.FACTORY.getRandomBuilder();

    public LiveQueryTable(TrackingMutableRowSet rowSet, Map<String, ? extends ColumnSource<?>> result) {
        super(rowSet, result);
    }

    @Override
    public void refresh() {
        final RowSetBuilder builder;
        synchronized (this) {
            builder = additionsBuilder;
            additionsBuilder = TrackingMutableRowSet.FACTORY.getRandomBuilder();
        }
        final TrackingMutableRowSet added = builder.build();
        getIndex().insert(added);
        if (added.size() > 0) {
            notifyListeners(added, TrackingMutableRowSet.FACTORY.getEmptyRowSet(), TrackingMutableRowSet.FACTORY.getEmptyRowSet());
        }
    }

    public synchronized void addIndex(long key) {
        additionsBuilder.addKey(key);
    }

    public synchronized void addRange(long firstKey, long lastKey) {
        additionsBuilder.addRange(firstKey, lastKey);
    }

    @Override
    public void destroy() {
        super.destroy();
        LiveTableMonitor.DEFAULT.removeTable(this);
    }
}
