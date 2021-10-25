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
        this.lastRightRowSet = RowSetFactoryImpl.INSTANCE.getEmptyRowSet();
    }

    @Override
    protected void process() {
        if (!firstSnapshot && rightListener.recordedVariablesAreValid()) {
            if (rightUpdates == null) {
                rightUpdates = new UpdateCoalescer(rightTable.getIndex(), rightListener.getUpdate());
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
        doRowCopy(rightTable.getIndex());
        resultTable.getIndex().insert(rightTable.getIndex());
        if (!initial) {
            resultTable.notifyListeners(resultTable.getIndex(), RowSetFactoryImpl.INSTANCE.getEmptyRowSet(),
                    RowSetFactoryImpl.INSTANCE.getEmptyRowSet());
        }
        firstSnapshot = false;
    }

    public void doSnapshot() {
        lastRightRowSet.clear();
        lastRightRowSet.insert(rightTable.getIndex());
        try (final IndexShiftDataExpander expander =
                new IndexShiftDataExpander(rightUpdates.coalesce(), lastRightRowSet)) {
            final TrackingMutableRowSet rightAdded = expander.getAdded();
            final TrackingMutableRowSet rightModified = expander.getModified();
            final TrackingMutableRowSet rightRemoved = expander.getRemoved();
            final TrackingMutableRowSet rowsToCopy = rightAdded.union(rightModified);

            doRowCopy(rowsToCopy);

            resultTable.getIndex().update(rightAdded, rightRemoved);
            resultTable.notifyListeners(rightAdded, rightRemoved, rightModified);
        }
    }

    private void doRowCopy(TrackingMutableRowSet rowSet) {
        copyRowsToResult(rowSet, triggerTable, rightTable, leftColumns, resultColumns);
    }

    public static void copyRowsToResult(TrackingMutableRowSet rowsToCopy, QueryTable triggerTable, QueryTable rightTable,
                                        Map<String, ? extends ColumnSource<?>> leftColumns, Map<String, SparseArrayColumnSource<?>> resultColumns) {
        final TrackingMutableRowSet qtRowSet = triggerTable.getIndex();
        if (!qtRowSet.isEmpty()) {
            SnapshotUtils.copyStampColumns(leftColumns, qtRowSet.lastRowKey(), resultColumns, rowsToCopy);
        }
        SnapshotUtils.copyDataColumns(rightTable.getColumnSourceMap(), rowsToCopy, resultColumns, rowsToCopy, false);
    }
}
