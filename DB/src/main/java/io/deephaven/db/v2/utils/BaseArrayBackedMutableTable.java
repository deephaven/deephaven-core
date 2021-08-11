package io.deephaven.db.v2.utils;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.util.config.InputTableStatusListener;
import io.deephaven.db.util.config.MutableInputTable;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.UpdatableTable;
import io.deephaven.db.v2.sources.ArrayBackedColumnSource;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.util.annotations.TestUseOnly;
import io.deephaven.web.shared.data.InputTableDefinition;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

abstract class BaseArrayBackedMutableTable extends UpdatableTable {

    private static final Object[] BOOLEAN_ENUM_ARRAY = new Object[]{true, false, null};

    protected final InputTableDefinition inputTableDefinition;
    private final List<PendingChange> pendingChanges = Collections.synchronizedList(new ArrayList<>());
    private final AtomicLong nextSequence = new AtomicLong(0);
    private final AtomicLong processedSequence = new AtomicLong(0);
    private final Map<String, Object[]> enumValues;

    private String description = getDefaultDescription();
    private Runnable onPendingChange = () -> LiveTableMonitor.DEFAULT.requestRefresh(this);

    long nextRow = 0;
    private long pendingProcessed = NULL_NOTIFICATION_STEP;

    public BaseArrayBackedMutableTable(Index index, Map<String, ? extends ColumnSource> nameToColumnSource, Map<String, Object[]> enumValues, ProcessPendingUpdater processPendingUpdater) {
        super(index, nameToColumnSource, processPendingUpdater);
        this.enumValues = enumValues;
        this.inputTableDefinition = new InputTableDefinition();
        MutableInputTable mutableInputTable = makeHandler();
        setAttribute(Table.INPUT_TABLE_ATTRIBUTE, mutableInputTable);
        setRefreshing(true);
        processPendingUpdater.setThis(this);
    }

    protected static Map<String, ? extends ArrayBackedColumnSource<?>> makeColumnSourceMap(TableDefinition definition) {
        final Map<String, ArrayBackedColumnSource<?>> resultMap = new LinkedHashMap<>();
        for (final ColumnDefinition<?> columnDefinition : definition.getColumns()) {
            resultMap.put(columnDefinition.getName(), ArrayBackedColumnSource.getMemoryColumnSource(0, columnDefinition.getDataType()));
        }
        return resultMap;
    }

    static void processInitial(Table initialTable, BaseArrayBackedMutableTable result) {
        final Index.SequentialBuilder builder = Index.FACTORY.getSequentialBuilder();
        result.processPendingTable(initialTable, true, new IndexChangeRecorder() {
            @Override
            public void addIndex(long key) {
                builder.appendKey(key);
            }

            @Override
            public void removeIndex(long key) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void modifyIndex(long key) {
                throw new UnsupportedOperationException();
            }
        }, (e) -> {});
        result.getIndex().insert(builder.getIndex());
        result.getIndex().initializePreviousValue();
        LiveTableMonitor.DEFAULT.addTable(result);
    }

    public BaseArrayBackedMutableTable setDescription(String newDescription) {
        this.description = newDescription;
        return this;
    }

    /**
     * For unit test use only. Specify the function to invoke after enqueuing a pending change.
     *
     * @param onPendingChange The function to invoke after enqueuing a pending change, or null to restore the default behavior
     */
    @TestUseOnly
    void setOnPendingChange(final Runnable onPendingChange) {
        this.onPendingChange = onPendingChange == null ? () -> LiveTableMonitor.DEFAULT.requestRefresh(this) : onPendingChange;
    }

    private void processPending(IndexChangeRecorder indexChangeRecorder) {
        synchronized (pendingChanges) {
            for (PendingChange pendingChange : pendingChanges) {
                if (pendingChange.delete) {
                    processPendingDelete(pendingChange.table, indexChangeRecorder);
                } else {
                    processPendingTable(pendingChange.table, pendingChange.allowEdits, indexChangeRecorder, (e) -> pendingChange.error = e);
                }
                pendingProcessed = pendingChange.sequence;
            }
            pendingChanges.clear();
        }
    }

    @Override
    public void refresh() {
        super.refresh();
        synchronized (pendingChanges) {
            processedSequence.set(pendingProcessed);
            pendingProcessed = NULL_NOTIFICATION_STEP;
            pendingChanges.notifyAll();
        }
    }

    protected abstract void processPendingTable(Table table, boolean allowEdits, IndexChangeRecorder indexChangeRecorder, Consumer<String> errorNotifier);

    protected abstract void processPendingDelete(Table table, IndexChangeRecorder indexChangeRecorder);

    protected abstract String getDefaultDescription();

    abstract void validateDelete(TableDefinition definition);

    private void validateDefinition(final TableDefinition newDefinition) {
        final TableDefinition thisDefinition = getDefinition();
        thisDefinition.checkCompatibility(newDefinition);
        newDefinition.checkCompatibility(thisDefinition);
    }

    protected static class ProcessPendingUpdater implements Updater {
        private BaseArrayBackedMutableTable baseArrayBackedMutableTable;

        @Override
        public void accept(IndexChangeRecorder indexChangeRecorder) {
            baseArrayBackedMutableTable.processPending(indexChangeRecorder);
        }

        public void setThis(BaseArrayBackedMutableTable keyedArrayBackedMutableTable) {
            this.baseArrayBackedMutableTable = keyedArrayBackedMutableTable;
        }
    }

    private final class PendingChange {
        final boolean delete;
        final Table table;
        final long sequence;
        final boolean allowEdits;
        String error;

        private PendingChange(Table table, boolean delete, boolean allowEdits) {
            this.table = table;
            this.delete = delete;
            this.allowEdits = allowEdits;
            this.sequence = nextSequence.incrementAndGet();
        }
    }

    ArrayBackedMutableInputTable makeHandler() {
        return new ArrayBackedMutableInputTable();
    }

    protected class ArrayBackedMutableInputTable implements MutableInputTable {
        @Override
        public InputTableDefinition getDefinition() {
            return inputTableDefinition;
        }

        @Override
        public TableDefinition getTableDefinition() {
            return BaseArrayBackedMutableTable.this.getDefinition();
        }

        @Override
        public void add(Table newData) {
            final long sequence = enqueueAddition(newData, true).sequence;
            waitForSequence(sequence);
        }

        private void add(Table newData, boolean allowEdits, InputTableStatusListener listener) {
            final PendingChange pendingChange = enqueueAddition(newData, allowEdits);
            CompletableFuture.runAsync(() -> waitForSequence(pendingChange.sequence)).thenAccept((v) -> {
                if (pendingChange.error == null) {
                    listener.onSuccess();
                } else {
                    listener.onError(new IllegalArgumentException(pendingChange.error));
                }
            }).exceptionally(ex -> {
                listener.onError(ex);
                return null;
            });
        }

        private PendingChange enqueueAddition(Table newData, boolean allowEdits) {
            validateDefinition(newData.getDefinition());
            // we want to get a clean copy of the table; that can not change out from under us or result in long reads
            // during our LTM refresh
            final PendingChange pendingChange = new PendingChange(doSnap(newData), false, allowEdits);
            pendingChanges.add(pendingChange);
            onPendingChange.run();
            return pendingChange;
        }

        private Table doSnap(Table newData, Index index) {
            return doSnap(newData.getSubTable(index));
        }

        private Table doSnap(Table newData) {
            Table addTable;
            if (newData.isLive()) {
                addTable = TableTools.emptyTable(1).snapshot(newData);
            } else {
                addTable = newData.select();
            }
            return addTable;
        }

        @Override
        public void delete(Table table, Index index) {
            validateDelete(table.getDefinition());
            final PendingChange pendingChange = new PendingChange(doSnap(table, index), true, false);
            pendingChanges.add(pendingChange);
            onPendingChange.run();
            waitForSequence(pendingChange.sequence);
        }

        @Override
        public String getDescription() {
            return description;
        }

        void waitForSequence(long sequence) {
            if (LiveTableMonitor.DEFAULT.exclusiveLock().isHeldByCurrentThread()) {
                // We're holding the lock. currentTable had better be a DynamicTable. Wait on its LTM condition
                // in order to allow updates.
                while (processedSequence.longValue() < sequence) {
                    try {
                        BaseArrayBackedMutableTable.this.awaitUpdate();
                    } catch (InterruptedException ignored) {
                    }
                }
            } else {
                // we are not holding the lock, so should wait for the next refresh
                synchronized (pendingChanges) {
                    while (processedSequence.longValue() < sequence) {
                        try {
                            pendingChanges.wait();
                        } catch (InterruptedException ignored) {
                        }
                    }
                }
            }
        }

        @Override
        public void setRows(@NotNull Table defaultValues, int[] rowArray, Map<String, Object>[] valueArray, InputTableStatusListener listener) {
            Assert.neqNull(defaultValues, "defaultValues");
            if (defaultValues.isLive()) {
                LiveTableMonitor.DEFAULT.checkInitiateTableOperation();
            }

            final List<ColumnDefinition> columnDefinitions = getTableDefinition().getColumnList();
            final Map<String, ArrayBackedColumnSource> sources = buildSourcesMap(valueArray.length, columnDefinitions);
            final String[] kabmtColumns = getTableDefinition().getColumnNames().toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
            final ArrayBackedColumnSource[] sourcesByPosition = Arrays.stream(kabmtColumns).map(sources::get).toArray(ArrayBackedColumnSource[]::new);

            final Set<String> missingColumns = new HashSet<>(getTableDefinition().getColumnNames());

            for (final Map.Entry<String, ? extends ColumnSource> entry : defaultValues.getColumnSourceMap().entrySet()) {
                final String colName = entry.getKey();
                if (!sources.containsKey(colName)) {
                    continue;
                }
                final ColumnSource cs = Require.neqNull(entry.getValue(), "defaultValue column source: " + colName);
                final ArrayBackedColumnSource dest = Require.neqNull(sources.get(colName), "destination column source: " + colName);

                final Index defaultValuesIndex = defaultValues.getIndex();
                for (int rr = 0; rr < rowArray.length; ++rr) {
                    final long key = defaultValuesIndex.get(rowArray[rr]);
                    dest.set(rr, cs.get(key));
                }

                missingColumns.remove(colName);
            }

            for (int ii = 0; ii < valueArray.length; ++ii) {
                final Map<String, Object> passedInValues = valueArray[ii];

                for (int cc = 0; cc < sourcesByPosition.length; cc++) {
                    final String colName = kabmtColumns[cc];
                    if (passedInValues.containsKey(colName)) {
                        sourcesByPosition[cc].set(ii, passedInValues.get(colName));
                    } else if (missingColumns.contains(colName)) {
                        throw new IllegalArgumentException("No value specified for " + colName + " row " + ii);
                    }
                }
            }

            final QueryTable newData = new QueryTable(getTableDefinition(), Index.FACTORY.getFlatIndex(valueArray.length), sources);
            add(newData, true, listener);
        }

        @Override
        public void addRows(Map<String, Object>[] valueArray, boolean allowEdits, InputTableStatusListener listener) {
            final List<ColumnDefinition> columnDefinitions = getTableDefinition().getColumnList();
            final Map<String, ArrayBackedColumnSource> sources = buildSourcesMap(valueArray.length, columnDefinitions);

            for (int rowNumber = 0; rowNumber < valueArray.length; rowNumber++) {
                final Map<String, Object> values = valueArray[rowNumber];
                for (final ColumnDefinition columnDefinition : columnDefinitions) {
                    //noinspection unchecked
                    sources.get(columnDefinition.getName()).set(rowNumber, values.get(columnDefinition.getName()));
                }

            }

            final QueryTable newData = new QueryTable(getTableDefinition(), Index.FACTORY.getFlatIndex(valueArray.length), sources);

            add(newData, allowEdits, listener);
        }

        @NotNull
        private Map<String, ArrayBackedColumnSource> buildSourcesMap(int capacity, List<ColumnDefinition> columnDefinitions) {
            final Map<String, ArrayBackedColumnSource> sources = new LinkedHashMap<>();
            for (final ColumnDefinition columnDefinition : columnDefinitions) {
                final ArrayBackedColumnSource memoryColumnSource = ArrayBackedColumnSource.getMemoryColumnSource(capacity, columnDefinition.getDataType());
                memoryColumnSource.ensureCapacity(capacity);
                sources.put(columnDefinition.getName(), memoryColumnSource);
            }
            return sources;
        }

        @Override
        public Object[] getEnumsForColumn(String columnName) {
            if (getTableDefinition().getColumn(columnName).getDataType() == Boolean.class) {
                return BOOLEAN_ENUM_ARRAY;
            }
            return enumValues.get(columnName);
        }

        @Override
        public Table getTable() {
            return BaseArrayBackedMutableTable.this;
        }

        @Override
        public boolean canEdit() {
            // TODO: Should we be more restrictive, or provide a mechanism for determining which users can edit this
            // table beyond "they have a handle to it"?
            return true;
        }
    }
}
