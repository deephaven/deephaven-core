package io.deephaven.db.v2.snapshot;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.NotificationQueue;
import io.deephaven.db.v2.BaseTable;
import io.deephaven.db.v2.LazySnapshotTable;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.sources.ArrayBackedColumnSource;
import io.deephaven.db.v2.sources.SingleValueColumnSource;
import io.deephaven.db.v2.utils.Index;

import java.util.Map;

public class SnapshotInternalListener extends BaseTable.ShiftAwareListenerImpl {
    private final QueryTable triggerTable;
    private final boolean lazySnapshot;
    private final Table snapshotTable;
    private long snapshotPrevLength;
    private final QueryTable result;
    private final Map<String, SingleValueColumnSource<?>> resultLeftColumns;
    private final Map<String, ArrayBackedColumnSource<?>> resultRightColumns;
    private final Index resultIndex;

    public SnapshotInternalListener(QueryTable triggerTable,
            boolean lazySnapshot,
            Table snapshotTable,
            QueryTable result,
            Map<String, SingleValueColumnSource<?>> resultLeftColumns,
            Map<String, ArrayBackedColumnSource<?>> resultRightColumns,
            Index resultIndex) {
        super("snapshot " + result.getColumnSourceMap().keySet().toString(), triggerTable, result);
        this.triggerTable = triggerTable;
        this.result = result;
        this.lazySnapshot = lazySnapshot;
        this.snapshotTable = snapshotTable;
        this.snapshotPrevLength = 0;
        this.resultLeftColumns = resultLeftColumns;
        this.resultRightColumns = resultRightColumns;
        this.resultIndex = resultIndex;
        manage(snapshotTable);
    }

    @Override
    public void onUpdate(final Update upstream) {
        doSnapshot(true, false);
    }

    public void doSnapshot(final boolean notifyListeners, final boolean usePrev) {
        if (lazySnapshot) {
            ((LazySnapshotTable) snapshotTable).refreshForSnapshot();
        }

        // Populate stamp columns from the triggering table
        if (!triggerTable.getIndex().empty()) {
            SnapshotUtils.copyStampColumns(triggerTable.getColumnSourceMap(), triggerTable.getIndex().lastKey(),
                    resultLeftColumns, 0);
        }
        final Index currentIndex = snapshotTable.getIndex();
        final long snapshotSize;
        try (final Index prevIndex = usePrev ? currentIndex.getPrevIndex() : null) {
            final Index snapshotIndex = prevIndex != null ? prevIndex : currentIndex;
            snapshotSize = snapshotIndex.size();
            if (!snapshotIndex.empty()) {
                try (final Index destIndex = Index.FACTORY.getIndexByRange(0, snapshotIndex.size() - 1)) {
                    SnapshotUtils.copyDataColumns(snapshotTable.getColumnSourceMap(),
                            snapshotIndex, resultRightColumns, destIndex, usePrev);
                }
            }
        }
        if (snapshotPrevLength < snapshotSize) {
            // If the table got larger then:
            // - added is (the suffix)
            // - modified is (the old index)
            // resultIndex updated (by including added) for next time
            final Index modifiedRange = resultIndex.clone();
            final Index addedRange = Index.FACTORY.getIndexByRange(snapshotPrevLength, snapshotSize - 1);
            resultIndex.insert(addedRange);
            if (notifyListeners) {
                result.notifyListeners(addedRange, Index.FACTORY.getEmptyIndex(), modifiedRange);
            }
        } else if (snapshotPrevLength > snapshotSize) {
            // If the table got smaller, then:
            // - removed is (the suffix)
            // - resultIndex updated (by removing 'removed') for next time
            // modified is (just use the new index)
            final Index removedRange = Index.FACTORY.getIndexByRange(snapshotSize, snapshotPrevLength - 1);
            resultIndex.remove(removedRange);
            if (notifyListeners) {
                result.notifyListeners(Index.FACTORY.getEmptyIndex(), removedRange, resultIndex);
            }
        } else if (notifyListeners) {
            // If the table stayed the same size, then modified = the index
            result.notifyListeners(Index.FACTORY.getEmptyIndex(), Index.FACTORY.getEmptyIndex(), resultIndex.clone());
        }
        snapshotPrevLength = snapshotTable.size();
    }

    @Override
    public boolean canExecute(final long step) {
        if (!lazySnapshot && snapshotTable instanceof NotificationQueue.Dependency) {
            return ((NotificationQueue.Dependency) snapshotTable).satisfied(step) && super.canExecute(step);
        }
        return super.canExecute(step);
    }
}
