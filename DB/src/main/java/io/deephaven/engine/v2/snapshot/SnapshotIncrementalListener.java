package io.deephaven.engine.v2.snapshot;

import io.deephaven.engine.v2.ListenerRecorder;
import io.deephaven.engine.v2.MergedListener;
import io.deephaven.engine.v2.QueryTable;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.sources.SparseArrayColumnSource;
import io.deephaven.engine.v2.utils.*;

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

    private UpdateCoalescer rightUpdates;
    private final TrackingMutableRowSet lastRightRowSet;
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
        this.lastRightRowSet = RowSetFactory.empty().convertToTracking();
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
        resultTable.getRowSet().mutableCast().insert(rightTable.getRowSet());
        if (!initial) {
            resultTable.notifyListeners(resultTable.getRowSet().clone(),
                    RowSetFactory.empty(), RowSetFactory.empty());
        }
        firstSnapshot = false;
    }

    public void doSnapshot() {
        lastRightRowSet.clear();
        lastRightRowSet.insert(rightTable.getRowSet());
        try (final RowSetShiftDataExpander expander =
                new RowSetShiftDataExpander(rightUpdates.coalesce(), lastRightRowSet)) {
            final RowSet rightAdded = expander.getAdded().clone();
            final RowSet rightModified = expander.getModified().clone();
            final RowSet rightRemoved = expander.getRemoved().clone();
            final RowSet rowsToCopy = rightAdded.union(rightModified);

            doRowCopy(rowsToCopy);

            resultTable.getRowSet().mutableCast().update(rightAdded, rightRemoved);
            resultTable.notifyListeners(rightAdded, rightRemoved, rightModified);
        }
    }

    private void doRowCopy(RowSet rowSet) {
        copyRowsToResult(rowSet, triggerTable, rightTable, leftColumns, resultColumns);
    }

    public static void copyRowsToResult(RowSet rowsToCopy, QueryTable triggerTable, QueryTable rightTable,
            Map<String, ? extends ColumnSource<?>> leftColumns, Map<String, SparseArrayColumnSource<?>> resultColumns) {
        final RowSet qtRowSet = triggerTable.getRowSet();
        if (!qtRowSet.isEmpty()) {
            SnapshotUtils.copyStampColumns(leftColumns, qtRowSet.lastRowKey(), resultColumns, rowsToCopy);
        }
        SnapshotUtils.copyDataColumns(rightTable.getColumnSourceMap(), rowsToCopy, resultColumns, rowsToCopy, false);
    }
}
