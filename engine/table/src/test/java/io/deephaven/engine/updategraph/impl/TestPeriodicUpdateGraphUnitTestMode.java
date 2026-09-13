//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.updategraph.impl;

import io.deephaven.util.thread.ThreadInitializationFactory;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static io.deephaven.engine.context.TestExecutionContext.OPERATION_INITIALIZATION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestPeriodicUpdateGraphUnitTestMode {

    private static final int UPDATE_THREADS = 4;
    private static final String UPDATE_EXECUTORS_GROUP = "PeriodicUpdateGraph-updateExecutors";

    /**
     * The notification processing threads that {@link PeriodicUpdateGraph#resetForUnitTests(boolean)} installs before a
     * test must be gone after the corresponding post-test reset; otherwise every test in a JVM accumulates another
     * {@code updateThreads} parked threads.
     */
    @Test
    public void testResetForUnitTestsStopsNotificationProcessorThreads() {
        final PeriodicUpdateGraph updateGraph = new PeriodicUpdateGraph("TestUnitTestModeThreads", true, 1000, 25,
                UPDATE_THREADS, ThreadInitializationFactory.NO_OP, OPERATION_INITIALIZATION);
        updateGraph.enableUnitTestMode();

        final Set<Thread> preExisting = Thread.getAllStackTraces().keySet();

        updateGraph.resetForUnitTests(false);
        assertEquals(UPDATE_THREADS, ownUpdateExecutorThreads(preExisting).size());

        updateGraph.resetForUnitTests(true);
        final List<Thread> leaked = ownUpdateExecutorThreads(preExisting);
        assertTrue("Leaked notification processing threads: " + leaked, leaked.isEmpty());
    }

    /**
     * The live notification processing threads that were not running before the update graph under test was reset.
     */
    private static List<Thread> ownUpdateExecutorThreads(@NotNull final Set<Thread> preExisting) {
        final List<Thread> result = new ArrayList<>();
        for (final Thread thread : Thread.getAllStackTraces().keySet()) {
            final ThreadGroup group = thread.getThreadGroup();
            if (group != null && UPDATE_EXECUTORS_GROUP.equals(group.getName()) && !preExisting.contains(thread)) {
                result.add(thread);
            }
        }
        return result;
    }
}
