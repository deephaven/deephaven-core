//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.process;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;

class ShutdownManagerImplTest {

    ShutdownManagerImpl manager;

    @BeforeEach
    void setUp() {
        manager = new ShutdownManagerImpl();
    }

    @Test
    void taskInvokeOrder() {
        final List<String> out = Collections.synchronizedList(new ArrayList<>());
        // Tasks are executed in category order, but within a category, are executed in first-in, last-out (stack) order
        manager.registerTask(ShutdownManager.OrderingCategory.FIRST, () -> out.add("FIRST-3"));
        manager.registerTask(ShutdownManager.OrderingCategory.FIRST, () -> out.add("FIRST-2"));
        manager.registerTask(ShutdownManager.OrderingCategory.FIRST, () -> out.add("FIRST-1"));
        manager.registerTask(ShutdownManager.OrderingCategory.MIDDLE, () -> out.add("MIDDLE-3"));
        manager.registerTask(ShutdownManager.OrderingCategory.MIDDLE, () -> out.add("MIDDLE-2"));
        manager.registerTask(ShutdownManager.OrderingCategory.MIDDLE, () -> out.add("MIDDLE-1"));
        manager.registerTask(ShutdownManager.OrderingCategory.LAST, () -> out.add("LAST-3"));
        manager.registerTask(ShutdownManager.OrderingCategory.LAST, () -> out.add("LAST-2"));
        manager.registerTask(ShutdownManager.OrderingCategory.LAST, () -> out.add("LAST-1"));
        assertThat(manager.tasksInvoked()).isFalse();
        assertThat(manager.maybeInvokeTasks()).isTrue();
        assertThat(manager.tasksInvoked()).isTrue();
        assertThat(out).containsExactly("FIRST-1", "FIRST-2", "FIRST-3", "MIDDLE-1", "MIDDLE-2", "MIDDLE-3", "LAST-1",
                "LAST-2", "LAST-3");
    }

    @Test
    @Timeout(5)
    void awaitTasksFinished() throws InterruptedException {
        doAwaitTasksFinished(false);
    }

    // If we intentionally break io.deephaven.util.process.ShutdownManagerImpl.newShutdownHookThread by remove the
    // `tasksFinished.await()`, this test _will_ occasionally fail, but it does take a number of tries. Not worth
    // running this a lot unless changing ShutdownManagerImpl.
    // @RepeatedTest(value = 10000, failureThreshold = 1)
    @Test
    @Timeout(5)
    void shutdownHookAwaitsInvokeFinished() throws InterruptedException {
        doAwaitTasksFinished(true);
    }

    private void doAwaitTasksFinished(boolean viaShutdownHook) throws InterruptedException {
        final CountDownLatch invokeStartedLatch = new CountDownLatch(1);
        final CountDownLatch invokeReturnLatch = new CountDownLatch(1);
        final boolean[] finished = new boolean[1];
        manager.registerTask(ShutdownManager.OrderingCategory.LAST, () -> {
            invokeStartedLatch.countDown();
            try {
                invokeReturnLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            finished[0] = true;
        });

        // We know nothing has invoked the tasks yet
        assertThat(manager.tasksInvoked()).isFalse();

        // Invoke the shutdown tasks off-thread
        new Thread(() -> manager.maybeInvokeTasks()).start();

        // Wait until we know task has been called
        invokeStartedLatch.await();

        // We know the task is currently blocked in ShutdownManager.Task.invoke
        assertThat(manager.tasksInvoked()).isTrue();
        assertThat(manager.maybeInvokeTasks()).isFalse();

        // Async signal to task that it can continue. (This does not _need_ to be async, but gives us a higher
        // likelihood that awaitInvokeFinished will block.)
        new Thread(invokeReturnLatch::countDown).start();

        // We know that if we have waited for all the invoke tasks to complete, we know that the following read
        // "happens-after" the `finished[0] = true;` setter.
        if (!viaShutdownHook) {
            manager.awaitTasksFinished();
        } else {
            // The shutdown hook semantics should also wait for invoke to finish in the case that something else called
            // maybeInvokeTasks.
            final Thread thread = manager.newShutdownHookThread();
            thread.start();
            thread.join();
        }
        assertThat(finished[0]).isTrue();

        // After all the tasks have been invoked, this should still be true
        assertThat(manager.tasksInvoked()).isTrue();
    }
}
