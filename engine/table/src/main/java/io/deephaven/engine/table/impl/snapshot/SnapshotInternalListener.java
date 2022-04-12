package io.deephaven.engine.table.impl.snapshot;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.LazySnapshotTable;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.SingleValueColumnSource;

import java.util.Map;

public class SnapshotInternalListener extends BaseTable.ListenerImpl {
    private final QueryTable triggerTable;
    private final boolean lazySnapshot;
    private final Table snapshotTable;
    private long snapshotPrevLength;
    private final QueryTable result;
    private final Map<String, SingleValueColumnSource<?>> resultLeftColumns;
    private final Map<String, ArrayBackedColumnSource<?>> resultRightColumns;
    private final Map<String, ? extends ColumnSource<?>> triggerStampColumns;
    private final Map<String, ChunkSource.WithPrev<? extends Values>> snapshotDataColumns;
    private final TrackingWritableRowSet resultRowSet;

    public SnapshotInternalListener(QueryTable triggerTable,
            boolean lazySnapshot,
            Table snapshotTable,
            QueryTable result,
            Map<String, SingleValueColumnSource<?>> resultLeftColumns,
            Map<String, ArrayBackedColumnSource<?>> resultRightColumns,
            TrackingWritableRowSet resultRowSet) {
        super("snapshot " + result.getColumnSourceMap().keySet(), triggerTable, result);
        this.triggerTable = triggerTable;
        this.result = result;
        this.lazySnapshot = lazySnapshot;
        this.snapshotTable = snapshotTable;
        this.snapshotPrevLength = 0;
        this.resultLeftColumns = resultLeftColumns;
        this.resultRightColumns = resultRightColumns;
        this.resultRowSet = resultRowSet;
        manage(snapshotTable);
        triggerStampColumns = SnapshotUtils.generateTriggerStampColumns(triggerTable);
        snapshotDataColumns = SnapshotUtils.generateSnapshotDataColumns(snapshotTable);
    }

    @Override
    public void onUpdate(final TableUpdate upstream) {
        doSnapshot(true, false);
    }

    public void doSnapshot(final boolean notifyListeners, final boolean usePrev) {
        if (lazySnapshot) {
            ((LazySnapshotTable) snapshotTable).refreshForSnapshot();
        }

        // Populate stamp columns from the triggering table
        if (!triggerTable.getRowSet().isEmpty()) {
            SnapshotUtils.copyStampColumns(triggerStampColumns, triggerTable.getRowSet().lastRowKey(),
                    resultLeftColumns);
        }
        final TrackingRowSet currentRowSet = snapshotTable.getRowSet();
        final long snapshotSize;
        try (final RowSet prevRowSet = usePrev ? currentRowSet.copyPrev() : null) {
            final RowSet snapshotRowSet = prevRowSet != null ? prevRowSet : currentRowSet;
            snapshotSize = snapshotRowSet.size();
            if (!snapshotRowSet.isEmpty()) {
                try (final RowSet destRowSet = RowSetFactory.fromRange(0, snapshotRowSet.size() - 1)) {
                    SnapshotUtils.copyDataColumns(snapshotDataColumns,
                            snapshotRowSet, resultRightColumns, destRowSet, usePrev);
                }
            }
        }
        if (snapshotPrevLength < snapshotSize) {
            // If the table got larger then:
            // - added is (the suffix)
            // - modified is (the old RowSet)
            // resultRowSet updated (by including added) for next time
            final RowSet addedRange = RowSetFactory.fromRange(snapshotPrevLength, snapshotSize - 1);
            resultRowSet.insert(addedRange);
            if (notifyListeners) {
                result.notifyListeners(addedRange, RowSetFactory.empty(), resultRowSet.copy());
            }
        } else if (snapshotPrevLength > snapshotSize) {
            // If the table got smaller, then:
            // - removed is (the suffix)
            // - resultRowSet updated (by removing 'removed') for next time
            // modified is (just use the new RowSet)
            final RowSet removedRange = RowSetFactory.fromRange(snapshotSize, snapshotPrevLength - 1);
            resultRowSet.remove(removedRange);
            if (notifyListeners) {
                result.notifyListeners(RowSetFactory.empty(), removedRange, resultRowSet.copy());
            }
        } else if (notifyListeners) {
            // If the table stayed the same size, then modified = the RowSet
            result.notifyListeners(RowSetFactory.empty(), RowSetFactory.empty(),
                    resultRowSet.copy());
        }
        snapshotPrevLength = snapshotTable.size();
    }

    @Override
    public boolean canExecute(final long step) {
        if (!lazySnapshot) {
            return snapshotTable.satisfied(step) && super.canExecute(step);
        }
        return super.canExecute(step);
    }

}
