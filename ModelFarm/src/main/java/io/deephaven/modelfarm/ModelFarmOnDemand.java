/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.modelfarm;

import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.process.ProcessEnvironment;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.v2.DynamicTable;
import io.deephaven.db.v2.NotificationStepSource;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.util.FunctionalInterfaces;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.Set;

/**
 * A ModelFarm implementation for evaluating a model upon request, retrieving a snapshot of data for
 * all keys under a single {@link LiveTableMonitor} lock.
 *
 * @param <KEYTYPE> The type of the keys (e.g. {@link io.deephaven.modelfarm.fitterfarm.FitScope}).
 * @param <DATATYPE> The type of the data (e.g.
 *        {@link io.deephaven.modelfarm.fitterfarm.futures.FuturesFitDataOptionPrices}.
 * @param <ROWDATAMANAGERTYPE> The type of the RowDataManager (e.g.
 *        {@link io.deephaven.modelfarm.fitterfarm.futures.FuturesFitDataManager}).
 */
public class ModelFarmOnDemand<KEYTYPE, DATATYPE, ROWDATAMANAGERTYPE extends RowDataManager<KEYTYPE, DATATYPE>>
    extends ModelFarmBase<DATATYPE> {

    private static final boolean LOG_PERF = Configuration.getInstance()
        .getBooleanWithDefault("ModelFarm.logModelFarmOnDemandPerformance", false);
    private static final Logger log = ProcessEnvironment.getDefaultLog(ModelFarmOnDemand.class);
    private static final FunctionalInterfaces.ThrowingBiConsumer<QueryDataRetrievalOperation, NotificationStepSource, RuntimeException> DO_LOCKED_FUNCTION =
        getDoLockedConsumer(GetDataLockType.LTM_READ_LOCK);

    private static class QueueAndCallback<DATATYPE> {
        private final Queue<DATATYPE> queue;
        private final Runnable callback;

        private QueueAndCallback(Queue<DATATYPE> queue, Runnable callback) {
            this.queue = queue;
            this.callback = callback;
        }
    }

    /**
     * A queue of queues. Each "inner queue" (the elements of the {@code execQueue} represents one
     * on-demand pricing request. The {@link #execute()} method will drain each inner queue before
     * removing that queue from the {@code execQueue}.
     *
     * Must always acquire lock on this object before using it.
     */
    private final Queue<QueueAndCallback<DATATYPE>> execQueue = new ArrayDeque<>(1024);

    /**
     * Create a multithreaded resource to execute data driven models on demand.
     *
     * @param nThreads number of worker threads.
     * @param model model to execute.
     */
    @SuppressWarnings("unused")
    public ModelFarmOnDemand(int nThreads, Model<DATATYPE> model) {
        super(nThreads, model);
    }

    /**
     * Submit a request to {@link Model#exec execute} the {@link #model}. Can be called either with
     * or without a LiveTableMonitor lock -- the decision of whether/how to acquire a lock is left
     * to the {@link #DO_LOCKED_FUNCTION}. All keys represented by the data in the
     * {@code dataManager} will be processed.
     *
     * @param dataManager The {@code RowDataManager} that will provide data for the pricing
     *        requests.
     * @param callback A callback function to run after all keys have been processed. Can be
     *        {@code null}, in which case it will be ignored.
     */
    @SuppressWarnings("unused")
    public void requestUpdate(ROWDATAMANAGERTYPE dataManager, Runnable callback) {
        requestUpdate(dataManager, callback, null);
    }

    /**
     * Submit a request to {@link Model#exec execute} the {@link #model}. Can be called either with
     * or without a LiveTableMonitor lock -- the decision of whether/how to acquire a lock is left
     * to the {@link #DO_LOCKED_FUNCTION}.
     *
     * @param dataManager The {@code RowDataManager} that will provide data for the pricing
     *        requests.
     * @param callback A callback function to run after all keys have been processed. Can be
     *        {@code null}, in which case it will be ignored.
     * @param keys They keys for which data should be passed to the model. If {@code keys == null},
     *        then all keys represented by the data in the {@code dataManager} will be processed.
     */
    @SuppressWarnings("WeakerAccess")
    public void requestUpdate(ROWDATAMANAGERTYPE dataManager, Runnable callback,
        Set<KEYTYPE> keys) {
        if (keys != null && keys.isEmpty()) {
            return;
        }

        final DynamicTable dataManagerTable = dataManager.table();

        final Queue<DATATYPE> dataToEval =
            new ArrayDeque<>(keys != null ? keys.size() : dataManagerTable.intSize());
        // get data for all keys under the same lock
        DO_LOCKED_FUNCTION.accept((usePrev) -> {
            final Index index = dataManagerTable.getIndex();

            if (index.empty()) {
                log.warn().append(ModelFarmOnDemand.class.getSimpleName() + ": ")
                    .append("Table is empty. Nothing to price.").endl();
                callback.run();
                return;
            }

            for (Index.Iterator iter = index.iterator(); iter.hasNext();) {
                final long idx = iter.nextLong();

                // if a `keys` set was provided, then only enqueue keys in the `dataManager` that
                // are also in the set.
                final boolean includeThisKey;
                if (keys != null) {
                    final KEYTYPE key =
                        usePrev ? dataManager.uniqueIdPrev(idx) : dataManager.uniqueIdCurrent(idx);
                    includeThisKey = keys.contains(key);
                } else {
                    includeThisKey = true;
                }

                if (includeThisKey) {
                    // Enqueue the key for pricing.
                    final DATATYPE data = dataManager.newData();
                    dataManager.loadData(data, idx, usePrev);
                    dataToEval.add(data);
                }
            }
        }, (NotificationStepSource) dataManagerTable);

        if (dataToEval.isEmpty()) {
            log.warn().append("ModelFarmOnDemand: dataToEval is empty!").endl();
            callback.run();
            return;
        }

        synchronized (execQueue) {
            // Enqueue the `dataToEval` -- i.e. the queue of all keys to price as part of this
            // request.
            execQueue.add(new QueueAndCallback<>(dataToEval, callback));
            execQueue.notifyAll();
        }
    }

    @Override
    protected void execute() throws InterruptedException {
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }

        final long t0 = System.nanoTime();

        final DATATYPE data;
        QueueAndCallback<DATATYPE> queueAndCallbackOneRequest; // A queue of all data pertaining to
                                                               // one request
        final boolean queueEmpty;
        synchronized (execQueue) {

            // Pull the active request:
            while ((queueAndCallbackOneRequest = execQueue.peek()) == null) {
                execQueue.wait();
            }

            // Pull the data for the next key out of the queue
            data = queueAndCallbackOneRequest.queue.poll();
            Assert.neqNull(data, "data");

            // If `data` was the data for the last key in this request, then remove this request
            // from the `execQueue`:
            queueEmpty = queueAndCallbackOneRequest.queue.isEmpty();
            if (queueEmpty) {
                execQueue.remove();
            }
        }

        // Evaluate the model:
        final long t1 = System.nanoTime();
        final long t2;

        if (data != null) {
            model.exec(data);
            t2 = System.nanoTime();
        } else {
            // should be impossible; data is only populated with
            // io.deephaven.modelfarm.RowDataManager.newData
            throw new IllegalStateException("Data is null!");
        }

        if (LOG_PERF) {
            log.warn("ModelFarmOnDemand.execute PERFORMANCE: all=" + (t2 - t0) / 1000 + " take="
                + (t1 - t0) / 100 + " exec=" + (t2 - t1) / 1000);
        }

        if (queueEmpty && queueAndCallbackOneRequest.callback != null) {
            queueAndCallbackOneRequest.callback.run();
        }
    }

    @Override
    protected void modelFarmStarted() {}

    @Override
    protected boolean isQueueEmpty() {
        synchronized (execQueue) {
            return execQueue.isEmpty();
        }
    }

}
