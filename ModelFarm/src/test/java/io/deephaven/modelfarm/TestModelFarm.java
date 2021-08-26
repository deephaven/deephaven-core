/*
 * Copyright (c) 2016-2021. Deephaven Data Labs and Patent Pending.
 */

package io.deephaven.modelfarm;

import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.DynamicTable;
import io.deephaven.db.v2.sources.ColumnSource;
import junit.framework.TestCase;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.deephaven.util.QueryConstants.NULL_LONG;

public class TestModelFarm extends TestCase {

    private final long testShutdownTimeoutSecs =
            Configuration.getInstance().getIntegerWithDefault("TestModelFarm.testShutdownTimeoutSecs", 1);
    private final int nModelFarmThreadsDefault = 8;

    /**
     * Ensure that the ModelFarm terminates immediately if it is shut down while not busy with an empty queue.
     */
    public void testModelFarmNoWorkShutdown() throws Exception {
        final ModelFarmTick<Long, MutableLong, RowDataManager<Long, MutableLong>> modelFarmTick =
                getModelFarmTick(nModelFarmThreadsDefault, 0, 0, null);

        Require.eq(modelFarmTick.getState(), "modelFarmTick.getState()", ModelFarmBase.State.WAITING);

        modelFarmTick.start();
        Require.eq(modelFarmTick.getState(), "modelFarmTick.getState()", ModelFarmBase.State.RUNNING);

        Thread.sleep(100); // Give the model farm threads a bit of time to start up and get far enough to wait on the
                           // queue.

        /* Since no work is being done, we should be able to terminate very quickly: */
        boolean terminated = modelFarmTick.shutdownAndAwaitTermination(testShutdownTimeoutSecs, TimeUnit.SECONDS);
        Require.requirement(terminated, "terminated == true", terminated, "terminated", testShutdownTimeoutSecs,
                "testShutdownTimeoutSecs");
        Require.eq(modelFarmTick.getState(), "modelFarmTick.getState()", ModelFarmBase.State.TERMINATED);
    }

    /**
     * Ensure that the ModelFarm drains its work queue and then exits when {@link ModelFarmBase#shutdown()} and
     * {@link ModelFarmBase#awaitTermination}, but not {@link ModelFarmBase#terminate()}, are called.
     */
    public void testModelFarmBusyShutdownNoTerminate() throws Exception {
        final long workerSleepMs = 1000 * 3;
        final int nModelFarmThreads = 2;
        final int nKeys = nModelFarmThreads / 2;
        final AtomicInteger successCounter = new AtomicInteger(0);
        final ModelFarmTick<Long, MutableLong, RowDataManager<Long, MutableLong>> modelFarmTick =
                getModelFarmTick(nModelFarmThreads, nKeys, workerSleepMs, successCounter);

        Require.eq(modelFarmTick.getState(), "modelFarmTick.getState()", ModelFarmBase.State.WAITING);

        modelFarmTick.start();
        Require.eq(modelFarmTick.getState(), "modelFarmTick.getState()", ModelFarmBase.State.RUNNING);

        Thread.sleep(100); // Give the model farm threads a bit of time to start up and get far enough to pick up some
                           // work.

        Require.eq(modelFarmTick.getState(), "modelFarmTick.getState()", ModelFarmBase.State.RUNNING);

        // Shut down the model farm. Since the worker threads will sleep for (workerSleepMs) milliseconds,
        // the model farm will still be busy -- so its state should be SHUTDOWN, not TERMINATED.
        modelFarmTick.shutdown();
        Require.eq(modelFarmTick.getState(), "modelFarmTick.getState()", ModelFarmBase.State.SHUTDOWN);

        // Test awaitTermination(), without calling terminate().
        // This should return true, we are waiting for (workerSleepMs * 2) milliseconds but all work should be finished
        // within (workerSleepMs) milliseconds.
        boolean terminated = modelFarmTick.awaitTermination(workerSleepMs * 2, TimeUnit.MILLISECONDS);
        Require.eqTrue(terminated, "terminated");
        Require.eq(modelFarmTick.getState(), "modelFarmTick.getState()", ModelFarmBase.State.TERMINATED);

        // Ensure all jobs completed successfully (i.e. were not interrupted)
        Require.eq(successCounter.get(), "successCounter.get()", nKeys, "nKeys");
    }

    /**
     * Ensure that the ModelFarm interrupts its worker threads, stops processing its queue, and terminates immediately
     * if it is {@link ModelFarm#terminate() terminated} while busy and with work in its queue.
     */
    public void testModelFarmBusyShutdownAndTerminate() throws Exception {
        final long workerSleepMs = 1000 * 3;
        final int nKeys = nModelFarmThreadsDefault * 8;
        final AtomicInteger successCounter = new AtomicInteger(0);
        final ModelFarmTick<Long, MutableLong, RowDataManager<Long, MutableLong>> modelFarmTick =
                getModelFarmTick(nModelFarmThreadsDefault, nKeys, workerSleepMs, successCounter);

        Require.eq(modelFarmTick.getState(), "modelFarmTick.getState()", ModelFarmBase.State.WAITING);

        modelFarmTick.start();
        Require.eq(modelFarmTick.getState(), "modelFarmTick.getState()", ModelFarmBase.State.RUNNING);

        Thread.sleep(100); // Give the model farm threads a bit of time to start up and get far enough to pick up some
                           // work.

        Require.eq(modelFarmTick.getState(), "modelFarmTick.getState()", ModelFarmBase.State.RUNNING);

        // Shut down the model farm. Since the worker threads will sleep for (workerSleepMs) milliseconds, and there is
        // work in the queue,
        // the model farm will still be busy -- so its state should be SHUTDOWN, not TERMINATED.
        modelFarmTick.shutdown();
        Require.eq(modelFarmTick.getState(), "modelFarmTick.getState()", ModelFarmBase.State.SHUTDOWN);

        // Test awaitTermination() before calling terminate(). This should return false, since terminate() was not
        // called,
        // and the ModelFarm should not drain its work queue until (workerSleepMs * nKeys / nModelFarmThreads)
        // milliseconds have passed.
        {
            boolean terminated = modelFarmTick.awaitTermination(workerSleepMs, TimeUnit.MILLISECONDS);
            Require.eqFalse(terminated, "terminated");
            Require.eq(modelFarmTick.getState(), "modelFarmTick.getState()", ModelFarmBase.State.SHUTDOWN);
        }


        // Test awaitTermination() after calling terminate(). The ModelFarm should shut down almost immediately calling
        // terminate().
        {
            modelFarmTick.terminate();
            final ModelFarmBase.State stateAfterTerminate = modelFarmTick.getState();
            Require.requirement(
                    stateAfterTerminate == ModelFarmBase.State.TERMINATING
                            || stateAfterTerminate == ModelFarmBase.State.TERMINATED,
                    "stateAfterTerminate == ModelFarmBase.State.TERMINATING || stateAfterTerminate == ModelFarmBase.State.TERMINATED",
                    stateAfterTerminate,
                    "stateAfterTerminate");

            // Await termination. Since terminate() interrupts all model farm threads, this should happen quickly.
            boolean terminated = modelFarmTick.awaitTermination(testShutdownTimeoutSecs, TimeUnit.SECONDS);
            Require.requirement(terminated, "terminated == true", terminated, "terminated", testShutdownTimeoutSecs,
                    "testShutdownTimeoutSecs");
            Require.eq(modelFarmTick.getState(), "modelFarmTick.getState()", ModelFarmBase.State.TERMINATED);

            // Most of the keys should not have finished, since we terminated early.
            Require.lt(successCounter.get(), "successCounter.get()", nKeys, "nKeys");
        }
    }

    @NotNull
    private ModelFarmTick<Long, MutableLong, RowDataManager<Long, MutableLong>> getModelFarmTick(final int nThreads,
            final int nKeys, final long sleepMs, AtomicInteger successCounter) {
        return new ModelFarmTick<>(
                nThreads,
                data -> {
                    try {
                        final long sleepEndTime = System.currentTimeMillis() + sleepMs;
                        long remainingMs;
                        while ((remainingMs = (sleepEndTime - System.currentTimeMillis())) > 0) {
                            Thread.sleep(remainingMs);
                        }
                        if (successCounter != null) {
                            // Track number of keys processed without being interrupted.
                            successCounter.getAndIncrement();
                        }
                    } catch (InterruptedException e) {
                        throw new RuntimeException("Interrupted!", e);
                    }
                },
                new RowDataManager<Long, MutableLong>() {
                    @Override
                    public DynamicTable table() {
                        return (DynamicTable) TableTools.emptyTable(nKeys).update("Value = ii");
                    }

                    @Override
                    public MutableLong newData() {
                        return new MutableLong(NULL_LONG);
                    }

                    @Override
                    public Long uniqueIdCurrent(long index) {
                        return index;
                    }

                    @Override
                    public Long uniqueIdPrev(long index) {
                        return index;
                    }

                    @Override
                    public void loadData(MutableLong data, long index, boolean usePrev) {
                        final ColumnSource<Long> columnSource = table().getColumnSource("Value", long.class);
                        data.setValue(usePrev ? columnSource.getPrevLong(index) : columnSource.getLong(index));
                    }
                },
                nKeys);
    }

}
