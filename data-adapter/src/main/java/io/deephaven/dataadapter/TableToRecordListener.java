//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter;
import io.deephaven.dataadapter.datafetch.bulk.TableDataArrayRetriever;
import io.deephaven.dataadapter.rec.MultiRowRecordAdapter;
import io.deephaven.dataadapter.rec.desc.RecordAdapterDescriptor;
import io.deephaven.engine.updategraph.TerminalNotification;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

/**
 * Listener that converts table updates to records of type {@code T}, then passes the records to an external consumer.
 *
 * @param <T> The data type for the records.
 */
public class TableToRecordListener<T> extends InstrumentedTableUpdateListenerAdapter {

    private final MultiRowRecordAdapter<T> recordAdapter;

    private final Consumer<T> recordConsumer;
    private final Consumer<T> removedRecordConsumer;
    private final IntConsumer recordsProcessedListener;

    private final Queue<TableUpdates> recordsQueue;
    private final AtomicBoolean isShutdown;

    @SuppressWarnings("FieldCanBeLocal")
    private final Thread processingThread;
    private final TableDataArrayRetriever tableDataArrayRetriever;

    /**
     * @param description A description for the listener (see
     *        {@link InstrumentedTableUpdateListenerAdapter#InstrumentedTableUpdateListenerAdapter})
     * @param table The table whose updates will be processed into records
     * @param recordAdapterDescriptor Descriptor for converting table data into records of type {@code T}
     * @param recordConsumer Listener notified of added or updated records
     * @param removeRecordConsumer Listener notified of removed records
     * @param processInitialData Whether to process the initial data in the {@code table}
     * @param async {@code true} to notify subscribers of updates off the UpdateGraphProcessor thread; {@code false} to
     *        notify them on the UpdateGraphProcessor thread (directly in {@link #onUpdate})
     */
    public TableToRecordListener(
            @NotNull final String description,
            @NotNull final Table table,
            @NotNull final RecordAdapterDescriptor<T> recordAdapterDescriptor,
            @NotNull final Consumer<T> recordConsumer,
            @Nullable final Consumer<T> removeRecordConsumer,
            final boolean processInitialData,
            final boolean async) {
        this(description, table, recordAdapterDescriptor, recordConsumer, removeRecordConsumer, processInitialData,
                async, null);
    }

    /**
     * @param description A description for the listener (see
     *        {@link InstrumentedTableUpdateListenerAdapter#InstrumentedTableUpdateListenerAdapter})
     * @param table The table whose updates will be processed into records
     * @param recordAdapterDescriptor Descriptor for converting table data into records of type {@code T}
     * @param recordConsumer Listener notified of added or updated records
     * @param removeRecordConsumer Listener notified of removed records
     * @param processInitialData Whether to process the initial data in the {@code table}
     * @param async {@code true} to notify subscribers of updates off the UpdateGraphProcessor thread; {@code false} to
     *        notify them on the UpdateGraphProcessor thread (directly in {@link #onUpdate})
     * @param recordsProcessedListener Listener notified when record processing is complete, after the
     *        {@link #recordConsumer}/{@link #removedRecordConsumer} are called for a batch of records. If not
     *        {@code null}, this is invoked at the end of {@link #onUpdate} when {@code async==false}, and after each
     *        time the queue is drained/processed when {@code async==true}. For example, a {@link TerminalNotification}
     *        can be added to the update graph for further processing at the end of the cycle.
     */
    public TableToRecordListener(
            @NotNull final String description,
            @NotNull final Table table,
            @NotNull final RecordAdapterDescriptor<T> recordAdapterDescriptor,
            @NotNull final Consumer<T> recordConsumer,
            @Nullable final Consumer<T> removeRecordConsumer,
            final boolean processInitialData,
            final boolean async,
            @Nullable final IntConsumer recordsProcessedListener) {
        super(description, table, false);
        this.recordConsumer = recordConsumer;
        this.removedRecordConsumer = removeRecordConsumer;
        this.recordsProcessedListener = recordsProcessedListener;

        recordAdapter = recordAdapterDescriptor.createMultiRowRecordAdapter(table);
        tableDataArrayRetriever = recordAdapter.getTableDataArrayRetriever();

        if (async) {
            isShutdown = new AtomicBoolean(false);
            recordsQueue = new ArrayDeque<>();
            processingThread = new Thread(() -> {
                while (!isShutdown.get()) {
                    // Drain the queue to an array, then process the updates without
                    // holding the lock (so LTM isn't blocked for long)
                    final TableUpdates[] tableUpdates;
                    synchronized (recordsQueue) {
                        if (recordsQueue.isEmpty()) {
                            try {
                                recordsQueue.wait();
                            } catch (InterruptedException e) {
                                isShutdown.set(true);
                                recordsQueue.clear();
                                throw new RuntimeException(e);
                            }
                        }

                        tableUpdates = recordsQueue.toArray(new TableUpdates[0]);
                        recordsQueue.clear();
                    }

                    if (tableUpdates.length > 0) {
                        // total number of records processed. removed rows and previous values for modified rows are
                        // only counted if the removedRecordConsumer is set. if is removedRecordConsumer is set, then
                        // modified rows are counted twice (once for the new values and once for the previous values).
                        int totalUpdatedRows = 0;
                        for (TableUpdates update : tableUpdates) {
                            totalUpdatedRows += update.nUpdates;
                            processUpdateRecords(update);
                        }

                        notifyUpdatesProcessed(totalUpdatedRows);
                    }
                }
            }, TableToRecordListener.class.getSimpleName() + "_processingThread-" + description);

            processingThread.start();
        } else {
            isShutdown = null;
            processingThread = null;
            recordsQueue = null;
        }

        if (processInitialData) {
            if (!getUpdateGraph().sharedLock().isHeldByCurrentThread()
                    && !getUpdateGraph().exclusiveLock().isHeldByCurrentThread()) {
                throw new IllegalStateException("Cannot process initial if UpdateGraphProcessor is not locked! " +
                        "Create the TableToRecordListener in a different context or use " +
                        "UpdateGraphProcessor.DEFAULT.sharedLock().computeLocked() to instantiate under the lock.");
            }

            final RowSet tableIndex = table.getRowSet();
            final int tableSize = tableIndex.intSize();

            // Read the data into arrays:
            final Object[] dataArraysInitialData = tableDataArrayRetriever.createDataArrays(tableSize);
            tableDataArrayRetriever.fillDataArrays(false, dataArraysInitialData, tableIndex);

            // Process the arrays of data into records:
            processUpdateRecords(UpdateType.ADDED_UPDATED, tableSize, dataArraysInitialData);
        }

        table.addUpdateListener(this);
    }

    private void notifyUpdatesProcessed(int nUpdates) {
        if (this.recordsProcessedListener != null) {
            this.recordsProcessedListener.accept(nUpdates);
        }
    }

    /**
     * See {@link #create(Table, RecordAdapterDescriptor, Consumer, Consumer)}.
     */
    public static <T> TableToRecordListener<T> create(Table table, RecordAdapterDescriptor<T> recordAdapterDescriptor,
            Consumer<T> recordConsumer) {
        return create(table, recordAdapterDescriptor, recordConsumer, null);
    }

    /**
     * Creates a {@code TableToRecordListener} with the provided table, record adapter, and record consumers.
     *
     * @param table Table to listen to
     * @param recordAdapterDescriptor Describes how to populate the record
     * @param recordConsumer Consumer to receive new/modified records
     * @param removedRecordConsumer Consumer to receive removed records
     * @param <T> The record data type
     * @return The {@code TableToRecordListener}
     */
    public static <T> TableToRecordListener<T> create(Table table, RecordAdapterDescriptor<T> recordAdapterDescriptor,
            Consumer<T> recordConsumer, Consumer<T> removedRecordConsumer) {
        final String recordTypeName = recordAdapterDescriptor.getEmptyRecord().getClass().getSimpleName();
        final boolean async = false;
        return new TableToRecordListener<>(
                "TableToRecordListener_" + recordTypeName,
                table,
                recordAdapterDescriptor,
                recordConsumer,
                removedRecordConsumer,
                true, async);
    }

    /**
     * Pull data out of tables and store it in arrays.
     *
     * @param upstream The updates from the upstream table
     */
    @Override
    public void onUpdate(TableUpdate upstream) {
        // noinspection resource
        final RowSet newRecordsIndex = upstream.added().union(upstream.modified());
        final int newRecordsSize = newRecordsIndex.intSize();

        final Object[] dataArraysAddModify = tableDataArrayRetriever.createDataArrays(newRecordsSize);
        tableDataArrayRetriever.fillDataArrays(false, dataArraysAddModify, newRecordsIndex);

        final boolean processRemoved = removedRecordConsumer != null;
        final Object[] dataArraysRemoved;
        final int removedRecordsSize;

        if (processRemoved) {
            final RowSet removedRecordsIndex = upstream.getModifiedPreShift().union(upstream.removed());
            removedRecordsSize = removedRecordsIndex.intSize();
            dataArraysRemoved = tableDataArrayRetriever.createDataArrays(removedRecordsSize);
            tableDataArrayRetriever.fillDataArrays(true, dataArraysRemoved, removedRecordsIndex);
        } else {
            removedRecordsSize = Integer.MIN_VALUE;
            dataArraysRemoved = null;
        }

        if (recordsQueue != null) {
            synchronized (recordsQueue) {
                Assert.eqFalse(isShutdown.get(), "isShutdown");
                recordsQueue.add(new TableUpdates(UpdateType.ADDED_UPDATED, newRecordsSize, dataArraysAddModify));
                if (processRemoved) {
                    recordsQueue
                            .add(new TableUpdates(UpdateType.REMOVED_REPLACED, removedRecordsSize, dataArraysRemoved));
                }
                recordsQueue.notify();
            }
        } else {
            processUpdateRecords(UpdateType.ADDED_UPDATED, newRecordsSize, dataArraysAddModify);
            if (processRemoved) {
                processUpdateRecords(UpdateType.REMOVED_REPLACED, removedRecordsSize, dataArraysRemoved);
            }

            notifyUpdatesProcessed(newRecordsSize + removedRecordsSize);
        }
    }

    private void processUpdateRecords(final TableUpdates tableDataUpdates) {
        processUpdateRecords(tableDataUpdates.updateType, tableDataUpdates.nUpdates, tableDataUpdates.dataArrays);
    }

    private void processUpdateRecords(@NotNull final UpdateType updateType, final int nUpdates,
            @NotNull final Object[] dataArrays) {
        final T[] records = recordAdapter.createRecordsFromData(dataArrays, nUpdates);
        final Consumer<T> updateConsumer =
                updateType == UpdateType.ADDED_UPDATED ? recordConsumer : removedRecordConsumer;
        for (T record : records) {
            updateConsumer.accept(record);
        }
    }

    /**
     * Shut down this {@code TableToRecordsListener} by removing the listener from its source table and, if running in
     * asynchronous mode, marking that it has been shut down so that the processing thread can exit.
     */
    public void shutdown() {
        super.source.removeUpdateListener(this);
        if (isShutdown != null) {
            isShutdown.set(true);
        }
    }

    public enum UpdateType {
        /**
         * Records from the current data in the table, i.e. current values in new rows or rows that were modified
         */
        ADDED_UPDATED,
        /**
         * Records from the previous data in the table, i.e. removed rows or the 'old' values in rows that were modified
         */
        REMOVED_REPLACED
    }

    public static class TableUpdates {

        public final UpdateType updateType;

        /**
         * The number of updates (i.e. the length of all arrays in {@link #dataArrays}
         */
        public final int nUpdates;

        /**
         * An array of typed arrays of data corresponding to the updates.
         */
        public final Object[] dataArrays;

        private TableUpdates(final UpdateType updateType, final int nUpdates, final Object[] dataArrays) {
            this.updateType = updateType;
            this.nUpdates = nUpdates;
            this.dataArrays = dataArrays;
        }
    }

}
