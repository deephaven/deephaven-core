/*
 * Copyright (c) 2016-2021. Deephaven Data Labs and Patent Pending.
 */

package io.deephaven.modelfarm;

import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.table.ColumnSource;
import junit.framework.TestCase;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CountDownLatch;
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
        final CountDownLatch latchThreadStart = new CountDownLatch(0);
        final CountDownLatch latchThreadComplete = new CountDownLatch(0);
        final ModelFarmTick<Long, MutableLong, RowDataManager<Long, MutableLong>> modelFarmTick =
                getModelFarmTick(nModelFarmThreadsDefault, 0, latchThreadStart, latchThreadComplete, null);

        Require.eq(modelFarmTick.getState(), "modelFarmTick.getState()", ModelFarmBase.State.WAITING);

        modelFarmTick.start();
        Require.eq(modelFarmTick.getState(), "modelFarmTick.getState()", ModelFarmBase.State.RUNNING);

        // wait on the threads to start up and get into the queue (latchThreadStart=0 => no wait)
        latchThreadStart.await();

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
        final int nModelFarmThreads = 2;
        final int nKeys = nModelFarmThreads / 2;
        final CountDownLatch latchThreadStart = new CountDownLatch(1);
        final CountDownLatch latchThreadComplete = new CountDownLatch(1);
        final AtomicInteger successCounter = new AtomicInteger(0);
        final ModelFarmTick<Long, MutableLong, RowDataManager<Long, MutableLong>> modelFarmTick =
                getModelFarmTick(nModelFarmThreads, nKeys, latchThreadStart, latchThreadComplete, successCounter);

        Require.eq(modelFarmTick.getState(), "modelFarmTick.getState()", ModelFarmBase.State.WAITING);

        modelFarmTick.start();
        Require.eq(modelFarmTick.getState(), "modelFarmTick.getState()", ModelFarmBase.State.RUNNING);

        // wait on the threads to start up and get into the queue (latchThreadStart=1 => wait on a thread to come up)
        latchThreadStart.await();

        Require.eq(modelFarmTick.getState(), "modelFarmTick.getState()", ModelFarmBase.State.RUNNING);

        // Shut down the model farm. Since the worker threads are blocked,
        // the model farm will still be busy -- so its state should be SHUTDOWN, not TERMINATED.
        modelFarmTick.shutdown();
        Require.eq(modelFarmTick.getState(), "modelFarmTick.getState()", ModelFarmBase.State.SHUTDOWN);

        // Test awaitTermination(), without calling terminate().
        // This should return true, since threads are finished.
        latchThreadComplete.countDown();
        boolean terminated = modelFarmTick.awaitTermination(testShutdownTimeoutSecs, TimeUnit.SECONDS);
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
        final int nKeys = nModelFarmThreadsDefault * 8;
        final CountDownLatch latchThreadStart = new CountDownLatch(nModelFarmThreadsDefault);
        final CountDownLatch latchThreadComplete = new CountDownLatch(1);
        final AtomicInteger successCounter = new AtomicInteger(0);
        final ModelFarmTick<Long, MutableLong, RowDataManager<Long, MutableLong>> modelFarmTick =
                getModelFarmTick(nModelFarmThreadsDefault, nKeys, latchThreadStart, latchThreadComplete,
                        successCounter);

        Require.eq(modelFarmTick.getState(), "modelFarmTick.getState()", ModelFarmBase.State.WAITING);

        modelFarmTick.start();
        Require.eq(modelFarmTick.getState(), "modelFarmTick.getState()", ModelFarmBase.State.RUNNING);

        // wait on the threads to start up and get into the queue (latchThreadStart=nModelFarmThreadsDefault => wait on
        // all threads to come up)
        latchThreadStart.await();

        Require.eq(modelFarmTick.getState(), "modelFarmTick.getState()", ModelFarmBase.State.RUNNING);

        // Shut down the model farm. Since the worker threads will sleep, and there is work in the queue,
        // the model farm will still be busy -- so its state should be SHUTDOWN, not TERMINATED.
        modelFarmTick.shutdown();
        Require.eq(modelFarmTick.getState(), "modelFarmTick.getState()", ModelFarmBase.State.SHUTDOWN);

        // Test awaitTermination() before calling terminate(). This should return false, since terminate() was not
        // called, and the ModelFarm should not drain its work queue since the threads have not been completed.
        {
            boolean terminated = modelFarmTick.awaitTermination(testShutdownTimeoutSecs, TimeUnit.SECONDS);
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
            final int nKeys, final CountDownLatch latchThreadStart, final CountDownLatch latchThreadComplete,
            AtomicInteger successCounter) {
        return new ModelFarmTick<>(
                nThreads,
                data -> {
                    try {
                        latchThreadStart.countDown();

                        if (successCounter != null) {
                            // Track number of keys processed without being interrupted.
                            successCounter.getAndIncrement();
                        }

                        latchThreadComplete.await();
                    } catch (InterruptedException e) {
                        System.out.println("TestModelFarm thread interrupted.");
                    }
                },
                new RowDataManager<>() {
                    @Override
                    public Table table() {
                        return TableTools.emptyTable(nKeys).update("Value = ii");
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
