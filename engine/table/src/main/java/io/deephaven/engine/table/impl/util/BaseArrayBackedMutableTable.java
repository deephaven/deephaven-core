/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.util;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.util.config.InputTableStatusListener;
import io.deephaven.engine.util.config.MutableInputTable;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.UpdatableTable;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.util.annotations.TestUseOnly;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

abstract class BaseArrayBackedMutableTable extends UpdatableTable {

    private static final Object[] BOOLEAN_ENUM_ARRAY = new Object[] {true, false, null};

    /**
     * Queue of pending changes. Only synchronized access is permitted.
     */
    private final List<PendingChange> pendingChanges = new ArrayList<>();
    /** The most recently enqueue change sequence. Only accessed under the monitor lock for {@code pendingChanges}. */
    private long enqueuedSequence = 0L;
    /**
     * The most recently processed change sequence. Only written under <em>both</em> the monitor lock for
     * {@code pendingChanges} <em>and</em> from an update thread. Only read under either the UPG's exclusive lock or the
     * monitor lock on {@code pendingChanges}.
     */
    private long processedSequence = 0L;

    private final Map<String, Object[]> enumValues;

    private String description = getDefaultDescription();
    private Runnable onPendingChange = () -> UpdateGraphProcessor.DEFAULT.requestRefresh();

    long nextRow = 0;
    private long pendingProcessed = -1L;

    public BaseArrayBackedMutableTable(TrackingRowSet rowSet, Map<String, ? extends ColumnSource<?>> nameToColumnSource,
            Map<String, Object[]> enumValues, ProcessPendingUpdater processPendingUpdater) {
        super(rowSet, nameToColumnSource, processPendingUpdater);
        this.enumValues = enumValues;
        MutableInputTable mutableInputTable = makeHandler();
        setAttribute(Table.INPUT_TABLE_ATTRIBUTE, mutableInputTable);
        setRefreshing(true);
        processPendingUpdater.setThis(this);
    }

    public MutableInputTable mutableInputTable() {
        return (MutableInputTable) getAttribute(Table.INPUT_TABLE_ATTRIBUTE);
    }

    public Table readOnlyCopy() {
        return copy(BaseArrayBackedMutableTable::applicableForReadOnly);
    }

    private static boolean applicableForReadOnly(String attributeName) {
        return !Table.INPUT_TABLE_ATTRIBUTE.equals(attributeName);
    }

    protected static Map<String, ? extends ArrayBackedColumnSource<?>> makeColumnSourceMap(TableDefinition definition) {
        final Map<String, ArrayBackedColumnSource<?>> resultMap = new LinkedHashMap<>();
        for (final ColumnDefinition<?> columnDefinition : definition.getColumns()) {
            resultMap.put(columnDefinition.getName(),
                    ArrayBackedColumnSource.getMemoryColumnSource(0, columnDefinition.getDataType()));
        }
        return resultMap;
    }

    static void processInitial(Table initialTable, BaseArrayBackedMutableTable result) {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        result.processPendingTable(initialTable, true, new RowSetChangeRecorder() {
            @Override
            public void addRowKey(long key) {
                builder.appendKey(key);
            }

            @Override
            public void removeRowKey(long key) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void modifyRowKey(long key) {
                throw new UnsupportedOperationException();
            }
        }, (e) -> {
        });
        result.getRowSet().writableCast().insert(builder.build());
        result.getRowSet().writableCast().initializePreviousValue();
        UpdateGraphProcessor.DEFAULT.addSource(result);
    }

    public BaseArrayBackedMutableTable setDescription(String newDescription) {
        this.description = newDescription;
        return this;
    }

    /**
     * For unit test use only. Specify the function to invoke after enqueuing a pending change.
     *
     * @param onPendingChange The function to invoke after enqueuing a pending change, or null to restore the default
     *        behavior
     */
    @TestUseOnly
    void setOnPendingChange(final Runnable onPendingChange) {
        this.onPendingChange =
                onPendingChange == null ? () -> UpdateGraphProcessor.DEFAULT.requestRefresh() : onPendingChange;
    }

    private void processPending(RowSetChangeRecorder rowSetChangeRecorder) {
        synchronized (pendingChanges) {
            for (PendingChange pendingChange : pendingChanges) {
                if (pendingChange.delete) {
                    processPendingDelete(pendingChange.table, rowSetChangeRecorder);
                } else {
                    processPendingTable(pendingChange.table, pendingChange.allowEdits, rowSetChangeRecorder,
                            (e) -> pendingChange.error = e);
                }
                pendingProcessed = pendingChange.sequence;
            }
            pendingChanges.clear();
        }
    }

    @Override
    public void run() {
        super.run();
        synchronized (pendingChanges) {
            processedSequence = pendingProcessed;
            pendingProcessed = -1L;
            pendingChanges.notifyAll();
        }
    }

    protected abstract void processPendingTable(Table table, boolean allowEdits,
            RowSetChangeRecorder rowSetChangeRecorder, Consumer<String> errorNotifier);

    protected abstract void processPendingDelete(Table table, RowSetChangeRecorder rowSetChangeRecorder);

    protected abstract String getDefaultDescription();

    protected abstract List<String> getKeyNames();

    protected static class ProcessPendingUpdater implements Updater {
        private BaseArrayBackedMutableTable baseArrayBackedMutableTable;

        @Override
        public void accept(RowSetChangeRecorder rowSetChangeRecorder) {
            baseArrayBackedMutableTable.processPending(rowSetChangeRecorder);
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
            Assert.holdsLock(pendingChanges, "pendingChanges");
            this.table = table;
            this.delete = delete;
            this.allowEdits = allowEdits;
            this.sequence = ++enqueuedSequence;
        }
    }

    ArrayBackedMutableInputTable makeHandler() {
        return new ArrayBackedMutableInputTable();
    }

    protected class ArrayBackedMutableInputTable implements MutableInputTable {
        @Override
        public List<String> getKeyNames() {
            return BaseArrayBackedMutableTable.this.getKeyNames();
        }

        @Override
        public TableDefinition getTableDefinition() {
            return BaseArrayBackedMutableTable.this.getDefinition();
        }

        @Override
        public void add(Table newData) throws IOException {
            PendingChange pendingChange = enqueueAddition(newData, true);
            final long sequence = pendingChange.sequence;
            waitForSequence(sequence);
            if (pendingChange.error != null) {
                throw new IOException(pendingChange.error);
            }
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
            validateAddOrModify(newData);
            // we want to get a clean copy of the table; that can not change out from under us or result in long reads
            // during our UGP run
            final Table newDataSnapshot = doSnap(newData);
            final PendingChange pendingChange;
            synchronized (pendingChanges) {
                pendingChange = new PendingChange(newDataSnapshot, false, allowEdits);
                pendingChanges.add(pendingChange);
            }
            onPendingChange.run();
            return pendingChange;
        }

        private Table doSnap(Table newData, TrackingRowSet rowSet) {
            return doSnap(newData.getSubTable(rowSet));
        }

        private Table doSnap(Table newData) {
            Table addTable;
            if (newData.isRefreshing()) {
                addTable = TableTools.emptyTable(1).snapshot(newData);
            } else {
                addTable = newData.select();
            }
            return addTable;
        }

        @Override
        public void delete(Table table, TrackingRowSet rowsToDelete) throws IOException {
            validateDelete(table);
            final Table oldDataSnapshot = doSnap(table, rowsToDelete);
            final PendingChange pendingChange;
            synchronized (pendingChanges) {
                pendingChange = new PendingChange(oldDataSnapshot, true, false);
                pendingChanges.add(pendingChange);
            }
            onPendingChange.run();
            waitForSequence(pendingChange.sequence);

            if (pendingChange.error != null) {
                throw new IOException(pendingChange.error);
            }
        }

        @Override
        public String getDescription() {
            return description;
        }

        void waitForSequence(long sequence) {
            if (UpdateGraphProcessor.DEFAULT.exclusiveLock().isHeldByCurrentThread()) {
                // We're holding the lock. currentTable had better be refreshing. Wait on its UGP condition
                // in order to allow updates.
                while (processedSequence < sequence) {
                    try {
                        BaseArrayBackedMutableTable.this.awaitUpdate();
                    } catch (InterruptedException ignored) {
                    }
                }
            } else {
                // we are not holding the lock, so should wait for the next run
                synchronized (pendingChanges) {
                    while (processedSequence < sequence) {
                        try {
                            pendingChanges.wait();
                        } catch (InterruptedException ignored) {
                        }
                    }
                }
            }
        }

        @Override
        public void setRows(@NotNull Table defaultValues, int[] rowArray, Map<String, Object>[] valueArray,
                InputTableStatusListener listener) {
            Assert.neqNull(defaultValues, "defaultValues");
            if (defaultValues.isRefreshing()) {
                UpdateGraphProcessor.DEFAULT.checkInitiateTableOperation();
            }

            final List<ColumnDefinition<?>> columnDefinitions = getTableDefinition().getColumns();
            final Map<String, ArrayBackedColumnSource<Object>> sources =
                    buildSourcesMap(valueArray.length, columnDefinitions);
            final String[] kabmtColumns =
                    getTableDefinition().getColumnNames().toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
            // noinspection unchecked
            final ArrayBackedColumnSource<Object>[] sourcesByPosition =
                    Arrays.stream(kabmtColumns).map(sources::get).toArray(ArrayBackedColumnSource[]::new);

            final Set<String> missingColumns = new HashSet<>(getTableDefinition().getColumnNames());

            for (final Map.Entry<String, ? extends ColumnSource<?>> entry : defaultValues.getColumnSourceMap()
                    .entrySet()) {
                final String colName = entry.getKey();
                if (!sources.containsKey(colName)) {
                    continue;
                }
                final ColumnSource<?> cs = Require.neqNull(entry.getValue(), "defaultValue column source: " + colName);
                final ArrayBackedColumnSource<Object> dest =
                        Require.neqNull(sources.get(colName), "destination column source: " + colName);

                final RowSet defaultValuesRowSet = defaultValues.getRowSet();
                for (int rr = 0; rr < rowArray.length; ++rr) {
                    final long key = defaultValuesRowSet.get(rowArray[rr]);
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

            final QueryTable newData = new QueryTable(getTableDefinition(),
                    RowSetFactory.flat(valueArray.length).toTracking(), sources);
            add(newData, true, listener);
        }

        @Override
        public void addRows(Map<String, Object>[] valueArray, boolean allowEdits, InputTableStatusListener listener) {
            final List<ColumnDefinition<?>> columnDefinitions = getTableDefinition().getColumns();
            final Map<String, ArrayBackedColumnSource<Object>> sources =
                    buildSourcesMap(valueArray.length, columnDefinitions);

            for (int rowNumber = 0; rowNumber < valueArray.length; rowNumber++) {
                final Map<String, Object> values = valueArray[rowNumber];
                for (final ColumnDefinition<?> columnDefinition : columnDefinitions) {
                    sources.get(columnDefinition.getName()).set(rowNumber, values.get(columnDefinition.getName()));
                }

            }

            final QueryTable newData = new QueryTable(getTableDefinition(),
                    RowSetFactory.flat(valueArray.length).toTracking(), sources);

            add(newData, allowEdits, listener);
        }

        @NotNull
        private Map<String, ArrayBackedColumnSource<Object>> buildSourcesMap(int capacity,
                List<ColumnDefinition<?>> columnDefinitions) {
            final Map<String, ArrayBackedColumnSource<Object>> sources = new LinkedHashMap<>();
            for (final ColumnDefinition<?> columnDefinition : columnDefinitions) {
                ArrayBackedColumnSource<?> cs = ArrayBackedColumnSource.getMemoryColumnSource(
                        capacity, columnDefinition.getDataType());
                // noinspection unchecked
                final ArrayBackedColumnSource<Object> memoryColumnSource = (ArrayBackedColumnSource<Object>) cs;
                memoryColumnSource.ensureCapacity(capacity);
                sources.put(columnDefinition.getName(), memoryColumnSource);
            }
            return sources;
        }

        @Override
        public Object[] getEnumsForColumn(String columnName) {
            if (getTableDefinition().getColumn(columnName).getDataType().equals(Boolean.class)) {
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
