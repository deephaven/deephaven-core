/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.snapshot;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.impl.ListenerRecorder;
import io.deephaven.engine.table.impl.MergedListener;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.SparseArrayColumnSource;
import io.deephaven.engine.table.impl.util.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class SnapshotIncrementalListener extends MergedListener {
    private final QueryTable triggerTable;
    private final QueryTable resultTable;
    private final Map<String, SparseArrayColumnSource<?>> resultColumns;
    private final ListenerRecorder baseListener;
    private final ListenerRecorder triggerListener;
    private final QueryTable baseTable;
    private final Map<String, ? extends ColumnSource<?>> triggerColumns;
    private final Map<String, ChunkSource.WithPrev<? extends Values>> snapshotDataColumns;

    private UpdateCoalescer baseUpdates;
    private final TrackingWritableRowSet lastBaseRowSet;
    private boolean firstSnapshot = true;

    public SnapshotIncrementalListener(
            QueryTable triggerTable,
            QueryTable resultTable,
            Map<String, SparseArrayColumnSource<?>> resultColumns,
            ListenerRecorder baseListener,
            ListenerRecorder triggerListener,
            QueryTable baseTable,
            Map<String, ? extends ColumnSource<?>> triggerColumns) {
        super(Arrays.asList(baseListener, triggerListener), Collections.emptyList(), "snapshotIncremental",
                resultTable);
        this.triggerTable = triggerTable;
        this.resultTable = resultTable;
        this.resultColumns = resultColumns;
        this.baseListener = baseListener;
        this.triggerListener = triggerListener;
        this.baseTable = baseTable;
        this.triggerColumns = triggerColumns;
        this.lastBaseRowSet = RowSetFactory.empty().toTracking();
        this.snapshotDataColumns = SnapshotUtils.generateSnapshotDataColumns(baseTable);
    }

    @Override
    protected void process() {
        if (!firstSnapshot && baseListener.recordedVariablesAreValid()) {
            if (baseUpdates == null) {
                baseUpdates = new UpdateCoalescer(baseTable.getRowSet(), baseListener.getUpdate());
            } else {
                baseUpdates.update(baseListener.getUpdate());
            }
        }

        if (triggerListener.recordedVariablesAreValid()) {
            if (firstSnapshot) {
                doFirstSnapshot(false);
            } else if (baseUpdates != null) {
                doSnapshot();
            }
            baseUpdates = null;
        }
    }

    public void doFirstSnapshot(boolean initial) {
        doRowCopy(baseTable.getRowSet());
        resultTable.getRowSet().writableCast().insert(baseTable.getRowSet());
        if (!initial) {
            resultTable.notifyListeners(resultTable.getRowSet().copy(),
                    RowSetFactory.empty(), RowSetFactory.empty());
        }
        firstSnapshot = false;
    }

    public void doSnapshot() {
        lastBaseRowSet.clear();
        lastBaseRowSet.insert(baseTable.getRowSet());
        try (final RowSetShiftDataExpander expander =
                new RowSetShiftDataExpander(baseUpdates.coalesce(), lastBaseRowSet)) {
            final RowSet baseAdded = expander.getAdded().copy();
            final RowSet baseModified = expander.getModified().copy();
            final RowSet baseRemoved = expander.getRemoved().copy();
            final RowSet rowsToCopy = baseAdded.union(baseModified);

            doRowCopy(rowsToCopy);

            resultTable.getRowSet().writableCast().update(baseAdded, baseRemoved);
            resultTable.notifyListeners(baseAdded, baseRemoved, baseModified);
        }
    }

    private void doRowCopy(RowSet rowSet) {
        copyRowsToResult(rowSet, triggerTable, snapshotDataColumns, triggerColumns, resultColumns);
    }

    public static void copyRowsToResult(RowSet rowsToCopy, QueryTable triggerTable,
            Map<String, ChunkSource.WithPrev<? extends Values>> snapshotDataColumns,
            Map<String, ? extends ColumnSource<?>> triggerColumns,
            Map<String, SparseArrayColumnSource<?>> resultColumns) {
        final RowSet qtRowSet = triggerTable.getRowSet();
        if (!qtRowSet.isEmpty()) {
            SnapshotUtils.copyStampColumns(triggerColumns, qtRowSet.lastRowKey(), resultColumns, rowsToCopy);
        }
        SnapshotUtils.copyDataColumns(snapshotDataColumns, rowsToCopy, resultColumns, rowsToCopy, false);
    }
}
