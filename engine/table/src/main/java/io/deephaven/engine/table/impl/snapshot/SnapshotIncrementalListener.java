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
    private final ListenerRecorder rightListener;
    private final ListenerRecorder leftListener;
    private final QueryTable rightTable;
    private final Map<String, ? extends ColumnSource<?>> leftColumns;
    private final Map<String, ChunkSource.WithPrev<? extends Values>> snapshotDataColumns;

    private UpdateCoalescer rightUpdates;
    private final TrackingWritableRowSet lastRightRowSet;
    private boolean firstSnapshot = true;

    public SnapshotIncrementalListener(QueryTable triggerTable, QueryTable resultTable,
            Map<String, SparseArrayColumnSource<?>> resultColumns,
            ListenerRecorder rightListener, ListenerRecorder leftListener, QueryTable rightTable,
            Map<String, ? extends ColumnSource<?>> leftColumns) {
        super(Arrays.asList(rightListener, leftListener), Collections.emptyList(), "snapshotIncremental", resultTable);
        this.triggerTable = triggerTable;
        this.resultTable = resultTable;
        this.resultColumns = resultColumns;
        this.rightListener = rightListener;
        this.leftListener = leftListener;
        this.rightTable = rightTable;
        this.leftColumns = leftColumns;
        this.lastRightRowSet = RowSetFactory.empty().toTracking();
        this.snapshotDataColumns = SnapshotUtils.generateSnapshotDataColumns(rightTable);
    }

    @Override
    protected void process() {
        if (!firstSnapshot && rightListener.recordedVariablesAreValid()) {
            if (rightUpdates == null) {
                rightUpdates = new UpdateCoalescer(rightTable.getRowSet(), rightListener.getUpdate());
            } else {
                rightUpdates.update(rightListener.getUpdate());
            }
        }

        if (leftListener.recordedVariablesAreValid()) {
            if (firstSnapshot) {
                doFirstSnapshot(false);
            } else if (rightUpdates != null) {
                doSnapshot();
            }
            rightUpdates = null;
        }
    }

    public void doFirstSnapshot(boolean initial) {
        doRowCopy(rightTable.getRowSet());
        resultTable.getRowSet().writableCast().insert(rightTable.getRowSet());
        if (!initial) {
            resultTable.notifyListeners(resultTable.getRowSet().copy(),
                    RowSetFactory.empty(), RowSetFactory.empty());
        }
        firstSnapshot = false;
    }

    public void doSnapshot() {
        lastRightRowSet.clear();
        lastRightRowSet.insert(rightTable.getRowSet());
        try (final RowSetShiftDataExpander expander =
                new RowSetShiftDataExpander(rightUpdates.coalesce(), lastRightRowSet)) {
            final RowSet rightAdded = expander.getAdded().copy();
            final RowSet rightModified = expander.getModified().copy();
            final RowSet rightRemoved = expander.getRemoved().copy();
            final RowSet rowsToCopy = rightAdded.union(rightModified);

            doRowCopy(rowsToCopy);

            resultTable.getRowSet().writableCast().update(rightAdded, rightRemoved);
            resultTable.notifyListeners(rightAdded, rightRemoved, rightModified);
        }
    }

    private void doRowCopy(RowSet rowSet) {
        copyRowsToResult(rowSet, triggerTable, snapshotDataColumns, leftColumns, resultColumns);
    }

    public static void copyRowsToResult(RowSet rowsToCopy, QueryTable triggerTable,
            Map<String, ChunkSource.WithPrev<? extends Values>> snapshotDataColumns,
            Map<String, ? extends ColumnSource<?>> leftColumns, Map<String, SparseArrayColumnSource<?>> resultColumns) {
        final RowSet qtRowSet = triggerTable.getRowSet();
        if (!qtRowSet.isEmpty()) {
            SnapshotUtils.copyStampColumns(leftColumns, qtRowSet.lastRowKey(), resultColumns, rowsToCopy);
        }
        SnapshotUtils.copyDataColumns(snapshotDataColumns, rowsToCopy, resultColumns, rowsToCopy, false);
    }
}
