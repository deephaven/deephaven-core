/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.modelfarm;

import io.deephaven.configuration.Configuration;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.modelfarm.util.KeyedPriorityBlockingQueue;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A multithreaded resource to execute data driven models. Every time a row of the data table ticks, the unique
 * identifier is queued for execution. Once the security reaches the top of the execution queue, the most recent data
 * for the unique identifier is used for execution.
 * <p>
 * This is useful for real-time processing, where executing unique identifiers as fast as possible is desired.
 * <p>
 * The execution priority is determined by how long it has been since the data changed. The prioritizer can be used to
 * bump a unique identifier to a higher execution priority.
 *
 * @param <KEYTYPE> unique ID key type
 * @param <DATATYPE> data type
 */
public class ModelFarmRealTime<KEYTYPE, DATATYPE, ROWDATAMANAGERTYPE extends RowDataManager<KEYTYPE, DATATYPE>>
        extends RDMModelFarm<KEYTYPE, DATATYPE, ROWDATAMANAGERTYPE> {

    private static final Logger log = LoggerFactory.getLogger(ModelFarmRealTime.class);
    private static final boolean LOG_PERF =
            Configuration.getInstance().getBooleanWithDefault("ModelFarm.logModelFarmRealTimePerformance", false);

    private final ModelFarmBase.GetDataLockType GETDATA_LOCK_TYPE = ModelFarmBase.GetDataLockType.valueOf(Configuration
            .getInstance().getStringWithDefault("ModelFarm.ModelFarmRealTime.getDataLockType", "LTM_READ_LOCK"));
    private final KeyedPriorityBlockingQueue<KEYTYPE> execQueue = new KeyedPriorityBlockingQueue<>();
    private final ExecPrioritizer<KEYTYPE, DATATYPE, ROWDATAMANAGERTYPE> prioritizer;
    private final ModelFarmBase.MostRecentDataGetter<KEYTYPE, DATATYPE> mostRecentDataGetter;
    private final Map<KEYTYPE, Integer> lastPriorityMap = new ConcurrentHashMap<>();
    private final Map<KEYTYPE, AtomicBoolean> isEvalingMap = new ConcurrentHashMap<>();


    /**
     * Create a multithreaded resource to execute data driven models.
     *
     * @param nThreads number of worker threads.
     * @param model model to execute.
     * @param dataManager interface for accessing and querying data contained in rows of a dynamic table.
     * @param prioritizer utility for computing the execution priority.
     */
    public ModelFarmRealTime(final int nThreads, final Model<DATATYPE> model, final ROWDATAMANAGERTYPE dataManager,
            final ExecPrioritizer<KEYTYPE, DATATYPE, ROWDATAMANAGERTYPE> prioritizer) {
        super(nThreads, model, dataManager);
        this.prioritizer = prioritizer;
        log.warn().append("ModelFarmRealTime lock type: ").append(GETDATA_LOCK_TYPE.toString()).endl();
        this.mostRecentDataGetter = getMostRecentDataFactory(GETDATA_LOCK_TYPE);
    }

    @Override
    protected void onDataUpdate(Index added, Index removed, Index modified) {
        updateQueue(added);
        updateQueue(modified);
    }

    private void updateQueue(final Index index) {
        for (Index.Iterator it = index.iterator(); it.hasNext();) {
            final long i = it.nextLong();
            final KEYTYPE key = dataManager.uniqueIdCurrent(i);
            final int priority = prioritizer == null ? 0 : prioritizer.priority(dataManager, i);

            final State state = getState();
            if (state == State.WAITING || state == State.RUNNING) {
                execQueue.enqueue(key, priority);
                lastPriorityMap.put(key, priority);
            }
        }
    }

    @Override
    protected boolean isQueueEmpty() {
        return execQueue.isEmpty();
    }

    @Override
    protected void execute() throws InterruptedException {
        final long t0 = System.nanoTime();
        final KEYTYPE key = execQueue.take();
        final long t1 = System.nanoTime();

        // Whether *any* thread is currently evaling for this symbol:
        final AtomicBoolean isEvaling = isEvalingMap.computeIfAbsent(key, k -> new AtomicBoolean(false));

        // If another thread is already evaling this key, then reenque the key and move on to the next key.
        if (!isEvaling.compareAndSet(false, true)) {
            final Integer priority = lastPriorityMap.get(key);
            execQueue.enqueue(key, priority == null ? Integer.MIN_VALUE : priority);
            final long t2 = System.nanoTime();

            if (LOG_PERF) {
                log.warn().append("ModelFarmRealTime.execute PERFORMANCE: isExec=false all=").append((t2 - t0) / 1000)
                        .append(" take=").append((t1 - t0) / 1000).append(" enqueue=").append((t2 - t1) / 1000).endl();
            }

            return;
        }

        try {
            final DATATYPE data = mostRecentDataGetter.get(key);
            final long t2 = System.nanoTime();

            final long t3;
            final long t4;

            if (data != null) {
                prioritizer.execHappened(data);
                t3 = System.nanoTime();
                model.exec(data);
                t4 = System.nanoTime();
            } else {
                t3 = t2;
                t4 = t2;
            }

            if (LOG_PERF) {
                log.warn().append("ModelFarmRealTime.execute PERFORMANCE: isExec=true all=").append((t4 - t0) / 1000)
                        .append(" take=").append((t1 - t0) / 1000).append(" get=").append((t2 - t1) / 1000)
                        .append(" prio=").append((t3 - t2) / 1000).append(" exec=" + (t4 - t3) / 1000).endl();
            }
        } finally {
            if (!isEvaling.compareAndSet(true, false)) {
                // noinspection ThrowFromFinallyBlock -- once a thread sets isEvaling to true for a key, no other thread
                // should set it to false.
                throw new IllegalStateException("isEvaling is false but should be true for key " + key);
            }
        }
    }

    /**
     * Request an update for the given {@code key} with the maximum priority level.
     *
     * @param key The key to update.
     * @return {@code true} if the {@code element} was newly inserted to the queue or reinserted with a higher priority,
     *         otherwise {@code false}.
     */
    @SuppressWarnings("unused")
    public boolean requestUpdateMaxPriority(KEYTYPE key) {
        return requestUpdate(key, Integer.MAX_VALUE);
    }

    /**
     * Request an update for the given {@code key} with the specified priority level.
     *
     * @param key The key to update.
     * @param priority The priority with which the key should be updated.
     * @return {@code true} if the {@code element} was newly inserted to the queue or reinserted with a higher priority,
     *         otherwise {@code false}.
     */
    @SuppressWarnings("WeakerAccess")
    public boolean requestUpdate(KEYTYPE key, int priority) {
        return execQueue.enqueue(key, priority);
    }
}
