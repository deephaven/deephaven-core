package io.deephaven.queryutil.dataadapter;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListener;
import io.deephaven.engine.table.impl.UpdatableTable;
import io.deephaven.engine.table.impl.util.AsyncClientErrorNotifier;
import io.deephaven.engine.table.impl.util.AsyncErrorLogger;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.queryutil.dataadapter.datafetch.bulk.TableDataArrayRetriever;
import io.deephaven.queryutil.dataadapter.rec.MultiRowRecordAdapter;
import io.deephaven.queryutil.dataadapter.rec.desc.RecordAdapterDescriptor;
import io.deephaven.time.DateTimeUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
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
public class TableToRecordListener<T> extends InstrumentedTableUpdateListener {

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
     * @param description             A description for the listener (see {@link InstrumentedTableUpdateListener#InstrumentedTableUpdateListener})
     * @param table                   The table whose updates will be processed into records
     * @param recordAdapterDescriptor Descriptor for converting table data into records of type {@code T}
     * @param recordConsumer          Listener notified of added or updated records
     * @param removeRecordConsumer    Listener notified of removed records
     * @param processInitialData      Whether to process the initial data in the {@code table}
     * @param async                   {@code true} to notify subscribers of updates off the UpdateGraphProcessor thread; {@code false}
     *                                to notify them on the UpdateGraphProcessor thread (directly in {@link #onUpdate})
     */
    public TableToRecordListener(
            @NotNull final String description,
            @NotNull final Table table,
            @NotNull final RecordAdapterDescriptor<T> recordAdapterDescriptor,
            @NotNull final Consumer<T> recordConsumer,
            @Nullable final Consumer<T> removeRecordConsumer,
            final boolean processInitialData,
            final boolean async
    ) {
        this(description, table, recordAdapterDescriptor, recordConsumer, removeRecordConsumer, processInitialData, async, null);
    }

    /**
     * @param description              A description for the listener (see {@link InstrumentedTableUpdateListener#InstrumentedTableUpdateListener})
     * @param table                    The table whose updates will be processed into records
     * @param recordAdapterDescriptor  Descriptor for converting table data into records of type {@code T}
     * @param recordConsumer           Listener notified of added or updated records
     * @param removeRecordConsumer     Listener notified of removed records
     * @param processInitialData       Whether to process the initial data in the {@code table}
     * @param async                    {@code true} to notify subscribers of updates off the UpdateGraphProcessor thread; {@code false}
     *                                 to notify them on the UpdateGraphProcessor thread (directly in {@link #onUpdate})
     * @param recordsProcessedListener Listener notified when record processing is complete, after the
     *                                 {@link #recordConsumer}/{@link #removedRecordConsumer} are called for a batch
     *                                 of records. If not {@code null}, this is invoked at the end of {@link #onUpdate}
     *                                 when {@code async==false}, and after each time the queue is drained & processed
     *                                 when {@code async==true}.
     */
    public TableToRecordListener(@NotNull final String description,
                                 @NotNull final Table table,
                                 @NotNull final RecordAdapterDescriptor<T> recordAdapterDescriptor,
                                 @NotNull final Consumer<T> recordConsumer,
                                 @Nullable final Consumer<T> removeRecordConsumer,
                                 final boolean processInitialData,
                                 final boolean async,
                                 @Nullable final IntConsumer recordsProcessedListener) {
        super(description, true);
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

                    final int nUpdates = tableUpdates.length;
                    if (nUpdates > 0) {
                        for (TableUpdates update : tableUpdates) {
                            processUpdateRecords(update);
                        }

                        notifyUpdatesProcessed(nUpdates);
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
            if (!UpdateGraphProcessor.DEFAULT.sharedLock().isHeldByCurrentThread() && !UpdateGraphProcessor.DEFAULT.exclusiveLock().isHeldByCurrentThread()) {
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

        table.listenForUpdates(this);
    }

    private void notifyUpdatesProcessed(int nUpdates) {
        if (this.recordsProcessedListener != null) {
            this.recordsProcessedListener.accept(nUpdates);
        }
    }

    /**
     * See {@link #create(Table, RecordAdapterDescriptor, Consumer, Consumer)}.
     */
    public static <T> TableToRecordListener<T> create(Table table, RecordAdapterDescriptor<T> recordAdapterDescriptor, Consumer<T> recordConsumer) {
        return create(table, recordAdapterDescriptor, recordConsumer, null);
    }

    /**
     * Creates a {@code TableToRecordListener} with the provided table, record adapter, and record consumers.
     *
     * @param table                   Table to listen to
     * @param recordAdapterDescriptor Describes how to populate the record
     * @param recordConsumer          Consumer to receive new/modified records
     * @param removedRecordConsumer   Consumer to receive removed records
     * @param <T>                     The record data type
     * @return The {@code TableToRecordListener}
     */
    public static <T> TableToRecordListener<T> create(Table table, RecordAdapterDescriptor<T> recordAdapterDescriptor, Consumer<T> recordConsumer, Consumer<T> removedRecordConsumer) {
        final String recordTypeName = recordAdapterDescriptor.getEmptyRecord().getClass().getSimpleName();
        final boolean async = false;
        return new TableToRecordListener<>(
                "TableToRecordListener_" + recordTypeName,
                table,
                recordAdapterDescriptor,
                recordConsumer,
                removedRecordConsumer,
                true, async
        );
    }

    /**
     * Pull data out of tables and store it in arrays.
     *
     * @param upstream The updates from the upstream table
     */
    @Override
    public void onUpdate(TableUpdate upstream) {
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
            removedRecordsSize = 0;
            dataArraysRemoved = null;
        }

        if (recordsQueue != null) {
            synchronized (recordsQueue) {
                Assert.eqFalse(isShutdown.get(), "isShutdown");
                recordsQueue.add(new TableUpdates(UpdateType.ADDED_UPDATED, newRecordsSize, dataArraysAddModify));
                if (processRemoved) {
                    recordsQueue.add(new TableUpdates(UpdateType.REMOVED, removedRecordsSize, dataArraysRemoved));
                }
                recordsQueue.notify();
            }
        } else {
            processUpdateRecords(UpdateType.ADDED_UPDATED, newRecordsSize, dataArraysAddModify);
            if (processRemoved) {
                processUpdateRecords(UpdateType.REMOVED, removedRecordsSize, dataArraysRemoved);
            }

            notifyUpdatesProcessed(newRecordsSize + removedRecordsSize);
        }
    }

    private void processUpdateRecords(final TableUpdates tableDataUpdates) {
        processUpdateRecords(tableDataUpdates.updateType, tableDataUpdates.nUpdates, tableDataUpdates.dataArrays);
    }

    private void processUpdateRecords(final UpdateType updateType, final int nUpdates, final Object[] dataArrays) {
        final T[] records = recordAdapter.createRecordsFromData(dataArrays, nUpdates);
        final Consumer<T> updateConsumer = updateType == UpdateType.ADDED_UPDATED ? recordConsumer : removedRecordConsumer;
        for (T record : records) {
            updateConsumer.accept(record);
        }
    }

    @Override
    protected void onFailureInternal(Throwable originalException, Entry sourceEntry) {
        try {
            AsyncErrorLogger.log(DateTimeUtils.currentTime(), sourceEntry, sourceEntry, originalException);
            AsyncClientErrorNotifier.reportError(originalException);
        } catch (IOException e) {
            throw new RuntimeException("Exception in " + sourceEntry.toString(), originalException);
        }
    }

    enum UpdateType {
        ADDED_UPDATED,
        REMOVED
    }

    private static class TableUpdates {
        private final UpdateType updateType;
        private final int nUpdates;
        private final Object[] dataArrays;

        private TableUpdates(UpdateType updateType, int nUpdates, Object[] dataArrays) {
            this.updateType = updateType;
            this.nUpdates = nUpdates;
            this.dataArrays = dataArrays;
        }
    }

}
