/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.table.ColumnSource;

import java.util.Map;

/**
 * A {@link QueryTable} that acts as an update source within the {@link UpdateGraphProcessor}, with {@link RowSet}
 * changes queued externally by a single producer.
 */
public class UpdateSourceQueryTable extends QueryTable implements Runnable {

    private RowSetBuilderRandom additionsBuilder = RowSetFactory.builderRandom();

    public UpdateSourceQueryTable(TrackingWritableRowSet rowSet, Map<String, ? extends ColumnSource<?>> result) {
        super(rowSet, result);
    }

    @Override
    public void run() {
        final RowSetBuilderRandom builder;
        synchronized (this) {
            builder = additionsBuilder;
            additionsBuilder = RowSetFactory.builderRandom();
        }
        final RowSet added = builder.build();
        getRowSet().writableCast().insert(added);
        if (added.size() > 0) {
            notifyListeners(added, RowSetFactory.empty(),
                    RowSetFactory.empty());
        }
    }

    public synchronized void addRowKey(final long rowKey) {
        additionsBuilder.addKey(rowKey);
    }

    public synchronized void addRowKeyRange(final long firstRowKey, final long lastRowKey) {
        additionsBuilder.addRange(firstRowKey, lastRowKey);
    }

    @Override
    public void destroy() {
        super.destroy();
        UpdateGraphProcessor.DEFAULT.removeSource(this);
    }
}
