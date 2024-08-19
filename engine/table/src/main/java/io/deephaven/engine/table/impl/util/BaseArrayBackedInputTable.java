//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.util.input.InputTableStatusListener;
import io.deephaven.engine.util.input.InputTableUpdater;
import io.deephaven.engine.table.impl.UpdatableTable;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.util.annotations.TestUseOnly;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;

abstract class BaseArrayBackedInputTable extends UpdatableTable {

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

    private String description = getDefaultDescription();
    private Runnable onPendingChange = updateGraph::requestRefresh;

    long nextRow = 0;
    private long pendingProcessed = -1L;

    public BaseArrayBackedInputTable(TrackingRowSet rowSet, Map<String, ? extends ColumnSource<?>> nameToColumnSource,
            ProcessPendingUpdater processPendingUpdater) {
        super(rowSet, nameToColumnSource, processPendingUpdater);
        InputTableUpdater inputTableUpdater = makeUpdater();
        setAttribute(Table.INPUT_TABLE_ATTRIBUTE, inputTableUpdater);
        setRefreshing(true);
        processPendingUpdater.setThis(this);
    }

    public InputTableUpdater inputTable() {
        return (InputTableUpdater) getAttribute(Table.INPUT_TABLE_ATTRIBUTE);
    }

    public Table readOnlyCopy() {
        return copy(BaseArrayBackedInputTable::applicableForReadOnly);
    }

    private static boolean applicableForReadOnly(String attributeName) {
        return !Table.INPUT_TABLE_ATTRIBUTE.equals(attributeName);
    }

    protected static Map<String, ? extends WritableColumnSource<?>> makeColumnSourceMap(TableDefinition definition) {
        final Map<String, WritableColumnSource<?>> resultMap = new LinkedHashMap<>();
        for (final ColumnDefinition<?> columnDefinition : definition.getColumns()) {
            resultMap.put(columnDefinition.getName(),
                    ArrayBackedColumnSource.getMemoryColumnSource(0, columnDefinition.getDataType()));
        }
        return resultMap;
    }

    static void processInitial(Table initialTable, BaseArrayBackedInputTable result) {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        result.processPendingTable(initialTable, new RowSetChangeRecorder() {
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
        });
        result.getRowSet().writableCast().insert(builder.build());
        result.getRowSet().writableCast().initializePreviousValue();
        result.getUpdateGraph().addSource(result);
    }

    public BaseArrayBackedInputTable setDescription(String newDescription) {
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
        this.onPendingChange = onPendingChange == null
                ? updateGraph::requestRefresh
                : onPendingChange;
    }

    private void processPending(RowSetChangeRecorder rowSetChangeRecorder) {
        synchronized (pendingChanges) {
            for (PendingChange pendingChange : pendingChanges) {
                if (pendingChange.delete) {
                    processPendingDelete(pendingChange.table, rowSetChangeRecorder);
                } else {
                    processPendingTable(pendingChange.table, rowSetChangeRecorder);
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
            if (pendingProcessed < 0) {
                return;
            }
            processedSequence = pendingProcessed;
            pendingProcessed = -1L;
            pendingChanges.notifyAll();
        }
    }

    protected abstract void processPendingTable(Table table, RowSetChangeRecorder rowSetChangeRecorder);

    protected abstract void processPendingDelete(Table table, RowSetChangeRecorder rowSetChangeRecorder);

    protected abstract String getDefaultDescription();

    protected abstract List<String> getKeyNames();

    protected static class ProcessPendingUpdater implements Updater {
        private BaseArrayBackedInputTable baseArrayBackedInputTable;

        @Override
        public void accept(RowSetChangeRecorder rowSetChangeRecorder) {
            baseArrayBackedInputTable.processPending(rowSetChangeRecorder);
        }

        public void setThis(BaseArrayBackedInputTable keyedArrayBackedMutableTable) {
            this.baseArrayBackedInputTable = keyedArrayBackedMutableTable;
        }
    }

    private final class PendingChange {
        final boolean delete;
        @NotNull
        final Table table;
        final long sequence;
        String error;

        private PendingChange(@NotNull Table table, boolean delete) {
            Assert.assertion(Thread.holdsLock(pendingChanges), "Thread.holdsLock(pendingChanges)");
            Assert.neqNull(table, "table");
            this.table = table;
            this.delete = delete;
            this.sequence = ++enqueuedSequence;
        }
    }

    ArrayBackedInputTableUpdater makeUpdater() {
        return new ArrayBackedInputTableUpdater();
    }

    protected class ArrayBackedInputTableUpdater implements InputTableUpdater {
        @Override
        public List<String> getKeyNames() {
            return BaseArrayBackedInputTable.this.getKeyNames();
        }

        @Override
        public TableDefinition getTableDefinition() {
            return BaseArrayBackedInputTable.this.getDefinition();
        }

        @Override
        public void add(@NotNull final Table newData) throws IOException {
            checkBlockingEditSafety();
            PendingChange pendingChange = enqueueAddition(newData);
            if (pendingChange != null) {
                blockingContinuation(pendingChange);
            }
        }

        @Override
        public void addAsync(
                @NotNull final Table newData,
                @NotNull final InputTableStatusListener listener) {
            checkAsyncEditSafety(newData);
            final PendingChange pendingChange = enqueueAddition(newData);
            if (pendingChange != null) {
                asynchronousContinuation(pendingChange, listener);
            } else {
                listener.onSuccess();
            }
        }

        private PendingChange enqueueAddition(@NotNull final Table newData) {
            validateAddOrModify(newData);
            // we want to get a clean copy of the table; that can not change out from under us or result in long reads
            // during our UGP run
            final Table newDataSnapshot = snapshotData(newData);
            if (newDataSnapshot.size() == 0) {
                return null;
            }
            final PendingChange pendingChange;
            synchronized (pendingChanges) {
                pendingChange = new PendingChange(newDataSnapshot, false);
                pendingChanges.add(pendingChange);
            }
            onPendingChange.run();
            return pendingChange;
        }

        @Override
        public void delete(@NotNull final Table table) throws IOException {
            checkBlockingEditSafety();
            final PendingChange pendingChange = enqueueDeletion(table);
            if (pendingChange != null) {
                blockingContinuation(pendingChange);
            }
        }

        @Override
        public void deleteAsync(
                @NotNull final Table table,
                @NotNull final InputTableStatusListener listener) {
            checkAsyncEditSafety(table);
            final PendingChange pendingChange = enqueueDeletion(table);
            if (pendingChange != null) {
                asynchronousContinuation(pendingChange, listener);
            } else {
                listener.onSuccess();
            }
        }

        private PendingChange enqueueDeletion(@NotNull final Table table) {
            validateDelete(table);
            final Table oldDataSnapshot = snapshotData(table);
            if (oldDataSnapshot.size() == 0) {
                return null;
            }
            final PendingChange pendingChange;
            synchronized (pendingChanges) {
                pendingChange = new PendingChange(oldDataSnapshot, true);
                pendingChanges.add(pendingChange);
            }
            onPendingChange.run();
            return pendingChange;
        }

        private Table snapshotData(@NotNull final Table data) {
            Table dataSnapshot;
            if (data.isRefreshing()) {
                dataSnapshot = data.snapshot();
            } else {
                dataSnapshot = data.select();
            }
            return dataSnapshot;
        }

        private void blockingContinuation(@NotNull final PendingChange pendingChange) throws IOException {
            waitForSequence(pendingChange.sequence);
            if (pendingChange.error != null) {
                throw new IOException(pendingChange.error);
            }
        }

        private void asynchronousContinuation(
                @NotNull final PendingChange pendingChange,
                @NotNull final InputTableStatusListener listener) {
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

        private void checkBlockingEditSafety() {
            if (updateGraph.currentThreadProcessesUpdates()) {
                throw new UnsupportedOperationException("Attempted to make a blocking input table edit from a listener "
                        + "or notification. This is unsupported, because it will block the update graph from making "
                        + "progress and hang indefinitely.");
            }
            if (updateGraph.sharedLock().isHeldByCurrentThread()) {
                throw new UnsupportedOperationException("Attempted to make a blocking input table edit while holding "
                        + "the update graph's shared lock. This is unsupported, because it will block the update graph "
                        + "from making progress and hang indefinitely.");
            }
        }

        private void checkAsyncEditSafety(@NotNull final Table changeData) {
            if (changeData.isRefreshing()
                    && changeData.getUpdateGraph().currentThreadProcessesUpdates()
                    && !changeData.satisfied(changeData.getUpdateGraph().clock().currentStep())) {
                throw new UnsupportedOperationException("Attempted to make an asynchronous input table edit from a "
                        + "listener or notification before the table of data to add or delete is satisfied on the "
                        + "current cycle. This is unsupported, because it may block the update graph from making "
                        + "progress or produce inconsistent results.");
            }
        }

        void waitForSequence(long sequence) {
            if (updateGraph.exclusiveLock().isHeldByCurrentThread()) {
                // We're holding the lock. currentTable had better be refreshing. Wait on its UGP condition
                // in order to allow updates.
                while (processedSequence < sequence) {
                    try {
                        BaseArrayBackedInputTable.this.awaitUpdate();
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
    }
}
