//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.updategraph;

import io.deephaven.base.log.LogOutput;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.util.SafeCloseable;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link WaitNotification}.
 */
public class TestWaitNotification {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    /**
     * A thread forbidden to wait for the update graph is refused a wait for a dependency that is not yet satisfied,
     * rather than waiting. Once the dependency is satisfied, the cycle completes normally.
     */
    @Test
    public void testForbiddenWaitForSatisfaction() throws Exception {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final AtomicBoolean satisfied = new AtomicBoolean();
        final AtomicInteger satisfactionChecks = new AtomicInteger();
        final NotificationQueue.Dependency dependency = new NotificationQueue.Dependency() {
            @Override
            public boolean satisfied(final long step) {
                satisfactionChecks.incrementAndGet();
                return satisfied.get();
            }

            @Override
            public UpdateGraph getUpdateGraph() {
                return updateGraph;
            }

            @Override
            public LogOutput append(final LogOutput logOutput) {
                return logOutput.append("TestWaitNotification dependency");
            }
        };

        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            updateGraph.startCycleForUnitTests(false);
            try {
                final long step = updateGraph.clock().currentStep();
                final Future<Boolean> waitResult = executor.submit(() -> {
                    try (final SafeCloseable ignored = UpdateGraphWaitGuard.forbidWaits()) {
                        return WaitNotification.waitForSatisfaction(step, dependency);
                    }
                });
                try {
                    waitResult.get(30, TimeUnit.SECONDS);
                    fail("Expected UpdateGraphWaitForbiddenException");
                } catch (final ExecutionException e) {
                    assertTrue(String.valueOf(e.getCause()),
                            e.getCause() instanceof UpdateGraphWaitForbiddenException);
                }
                satisfied.set(true);
            } finally {
                updateGraph.markSourcesRefreshedForUnitTests();
                updateGraph.completeCycleForUnitTests();
            }
            // the refused wait's notification was delivered once its dependency was satisfied
            assertTrue(satisfactionChecks.get() > 0);
        } finally {
            executor.shutdownNow();
        }
    }
}
