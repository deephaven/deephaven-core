package io.deephaven.db.v2.snapshot;

import io.deephaven.db.v2.ListenerRecorder;
import io.deephaven.db.v2.MergedListener;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.SparseArrayColumnSource;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.IndexShiftDataExpander;

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

    private Index.IndexUpdateCoalescer rightUpdates;
    private final Index lastRightIndex;
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
        this.lastRightIndex = Index.FACTORY.getEmptyIndex();
    }

    @Override
    protected void process() {
        if (!firstSnapshot && rightListener.recordedVariablesAreValid()) {
            if (rightUpdates == null) {
                rightUpdates = new Index.IndexUpdateCoalescer(rightTable.getIndex(), rightListener.getUpdate());
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
            resultTable.notifyListeners(resultTable.getIndex(), Index.FACTORY.getEmptyIndex(),
                    Index.FACTORY.getEmptyIndex());
        }
        firstSnapshot = false;
    }

    public void doSnapshot() {
        lastRightIndex.clear();
        lastRightIndex.insert(rightTable.getIndex());
        try (final IndexShiftDataExpander expander =
                new IndexShiftDataExpander(rightUpdates.coalesce(), lastRightIndex)) {
            final Index rightAdded = expander.getAdded();
            final Index rightModified = expander.getModified();
            final Index rightRemoved = expander.getRemoved();
            final Index rowsToCopy = rightAdded.union(rightModified);

            doRowCopy(rowsToCopy);

            resultTable.getIndex().update(rightAdded, rightRemoved);
            resultTable.notifyListeners(rightAdded, rightRemoved, rightModified);
        }
    }

    private void doRowCopy(Index index) {
        copyRowsToResult(index, triggerTable, rightTable, leftColumns, resultColumns);
    }

    public static void copyRowsToResult(Index rowsToCopy, QueryTable triggerTable, QueryTable rightTable,
            Map<String, ? extends ColumnSource<?>> leftColumns, Map<String, SparseArrayColumnSource<?>> resultColumns) {
        final Index qtIndex = triggerTable.getIndex();
        if (!qtIndex.empty()) {
            SnapshotUtils.copyStampColumns(leftColumns, qtIndex.lastKey(), resultColumns, rowsToCopy);
        }
        SnapshotUtils.copyDataColumns(rightTable.getColumnSourceMap(), rowsToCopy, resultColumns, rowsToCopy, false);
    }
}
