/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.modelfarm;

import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;


/**
 * A multithreaded resource to execute data driven models. Every time a row of the data table ticks, the unique
 * identifier is executed using the data from the tick. Successive executions for a unique identifier are processed in
 * order.
 * <p>
 * This is useful for executing against historical data at a regular interval.
 *
 * @param <KEYTYPE> unique ID key type
 * @param <DATATYPE> data type
 */
public class ModelFarmTick<KEYTYPE, DATATYPE, ROWDATAMANAGERTYPE extends RowDataManager<KEYTYPE, DATATYPE>>
        extends RDMModelFarm<KEYTYPE, DATATYPE, ROWDATAMANAGERTYPE> {

    private static final Logger log = LoggerFactory.getLogger(ModelFarmTick.class);
    private static final boolean LOG_PERF =
            Configuration.getInstance().getBooleanWithDefault("ModelFarm.logModelFarmTickPerformance", false);
    private final int maxQueueSize;

    /**
     * A Queue for a unique identifier.
     * <p>
     * The queue uses a lock to ensure that only one item in the UQueue executes at a given time. Some models are
     * stateful, and the order of input data matters. This locking ensures that models receive data in an expected order
     * (e.g. time).
     *
     * @param <T> data type.
     */
    private static class UQueue<T> {
        private final ReentrantLock lock = new ReentrantLock();
        private final Queue<T> queue = new ConcurrentLinkedQueue<>();
    }

    private final Map<KEYTYPE, UQueue<DATATYPE>> umap = new ConcurrentHashMap<>();
    private final Queue<UQueue<DATATYPE>> queue;
    private final MostRecentDataGetter<KEYTYPE, DATATYPE> mostRecentDataGetter;


    /**
     * Create a multithreaded resource to execute data driven models.
     *
     * @param nThreads number of worker threads.
     * @param model model to execute.
     * @param dataManager interface for accessing and querying data contained in rows of a dynamic table.
     * @param maxQueueSize number of elements in the work queue backlog before the blocking new updates.
     */
    public ModelFarmTick(final int nThreads, final Model<DATATYPE> model, final ROWDATAMANAGERTYPE dataManager,
            final int maxQueueSize) {
        super(nThreads, model, dataManager);
        this.maxQueueSize = maxQueueSize;
        this.queue = new ArrayDeque<>(this.maxQueueSize);
        this.mostRecentDataGetter = getMostRecentDataFactory(GetDataLockType.LTM_LOCK_ALREADY_HELD);
    }

    @Override
    protected void onDataUpdate(Index added, Index removed, Index modified) {
        final Set<KEYTYPE> keys = new HashSet<>();

        for (Index.Iterator it = added.iterator(); it.hasNext();) {
            final long i = it.nextLong();
            final KEYTYPE key = dataManager.uniqueIdCurrent(i);
            keys.add(key);
        }

        for (Index.Iterator it = modified.iterator(); it.hasNext();) {
            final long i = it.nextLong();
            final KEYTYPE key = dataManager.uniqueIdCurrent(i);
            keys.add(key);
        }

        updateQueue(keys);
    }

    private void updateQueue(final Set<KEYTYPE> keys) {
        for (KEYTYPE key : keys) {
            final DATATYPE data = mostRecentDataGetter.get(key);

            synchronized (this) {
                final State state = getState();
                final boolean isNotShutdown = state == State.WAITING || state == State.RUNNING;

                if (data != null && isNotShutdown) {
                    final UQueue<DATATYPE> uqueue = umap.computeIfAbsent(key, u -> new UQueue<>());
                    uqueue.queue.add(data);

                    while (queue.size() >= maxQueueSize || !queue.offer(uqueue)) {
                        if (getState() == State.WAITING) {
                            throw new IllegalStateException(
                                    "Queue is full, but model farm is not started! Possible deadlock.  Consider increasing maxQueueSize: maxQueueSize="
                                            + maxQueueSize);
                        }

                        try {
                            this.wait();
                        } catch (InterruptedException e) {
                            log.warn(e).append("Interruption").endl();
                            throw new RuntimeException(e);
                        }
                    }

                    this.notifyAll();
                }
            }
        }
    }

    @Override
    protected synchronized boolean isQueueEmpty() {
        return queue.isEmpty();
    }

    @Override
    protected void execute() throws InterruptedException {
        final long t0 = System.nanoTime();

        UQueue<DATATYPE> uqueue;
        synchronized (this) {
            while ((uqueue = queue.poll()) == null) {
                if (getState() != State.RUNNING) {
                    return;
                } else {
                    this.wait();
                }
            }

            // notify that the queue has changed, so that other threads can check if queue is empty, and so that
            // updateQueue() can see if queue has more capacity
            this.notifyAll();
        }

        final long t1 = System.nanoTime();

        long t2, t3, t4, t5, t6;
        try {
            uqueue.lock.lock();
            t2 = System.nanoTime();
            final DATATYPE data = uqueue.queue.poll();
            t3 = System.nanoTime();
            Assert.neqNull(data, "data");
            model.exec(data);
            t4 = System.nanoTime();
        } finally {
            t5 = System.nanoTime();
            uqueue.lock.unlock();
            t6 = System.nanoTime();
        }

        final long t7 = System.nanoTime();

        if (LOG_PERF) {
            log.warn().append("ModelFarmTick.execute PERFORMANCE: all=").append((t7 - t0) / 1000).append(" take=")
                    .append((t1 - t0) / 1000).append(" lock=" + (t2 - t1) / 1000).append(" poll=")
                    .append((t3 - t2) / 1000)
                    .append(" exec=").append((t4 - t3) / 1000).append(" unlock=").append((t6 - t5) / 1000).endl();
        }

    }

}
