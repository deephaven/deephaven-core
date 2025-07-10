//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.impl.util.ImmediateJobScheduler;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.engine.table.impl.util.UpdateGraphJobScheduler;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.testutil.testcase.FakeProcessEnvironment;
import io.deephaven.engine.updategraph.UpdateGraph;
import junit.framework.TestCase;
import org.junit.Rule;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.junit.Assert.assertNotNull;

public final class TestJobScheduler {

    @Rule
    public final EngineCleanup cleanup = new EngineCleanup();

    @Test
    public void testParallel() throws InterruptedException, TimeoutException {
        final boolean[] completed = new boolean[100];
        final Observer observer = new Observer(() -> {
            // verify the set for the first 50
            for (int ii = 0; ii < 50; ii++) {
                Assert.eqTrue(completed[ii], "completed[" + ii + "]");
            }
            for (int ii = 50; ii < completed.length; ii++) {
                Assert.eqFalse(completed[ii], "completed[" + ii + "]");
            }
        }, null, null);
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.resetForUnitTests(false, true, 0, 4, 10, 5);
        // verify the set for the first 50
        updateGraph.runWithinUnitTestCycle(() -> {
            final JobScheduler scheduler = new UpdateGraphJobScheduler(updateGraph);
            scheduler.iterateParallel(
                    ExecutionContext.getContext(),
                    null,
                    JobScheduler.DEFAULT_CONTEXT_FACTORY,
                    0,
                    50,
                    (context, idx, nec) -> completed[idx] = true,
                    observer::onComplete,
                    observer::cleanup,
                    observer::onError);
        });
        observer.awaitFinished(Duration.ofSeconds(10));
        observer.assertSuccess();
    }

    @Test
    public void testParallelWithResume() throws InterruptedException, TimeoutException {
        final boolean[] completed = new boolean[100];
        final Observer observer = new Observer(() -> {
            // verify the set for the first 50
            for (int ii = 0; ii < 50; ii++) {
                Assert.eqTrue(completed[ii], "completed[" + ii + "]");
            }
            for (int ii = 50; ii < completed.length; ii++) {
                Assert.eqFalse(completed[ii], "completed[" + ii + "]");
            }
        }, null, null);
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.resetForUnitTests(false, true, 0, 4, 10, 5);
        // verify the set for the first 50
        updateGraph.runWithinUnitTestCycle(() -> {
            final JobScheduler scheduler = new UpdateGraphJobScheduler(updateGraph);
            scheduler.iterateParallel(
                    ExecutionContext.getContext(),
                    null,
                    JobScheduler.DEFAULT_CONTEXT_FACTORY,
                    0,
                    50,
                    (context, idx, nec, resume) -> {
                        completed[idx] = true;
                        resume.run();
                    },
                    observer::onComplete,
                    observer::cleanup,
                    observer::onError);
        });
        observer.awaitFinished(Duration.ofSeconds(10));
        observer.assertSuccess();
    }

    @Test
    public void testParallelWithContext() throws InterruptedException, TimeoutException {
        final boolean[] completed = new boolean[100];
        final Observer observer = new Observer(() -> {
            // verify true for the first 50
            for (int ii = 0; ii < 50; ii++) {
                Assert.eqTrue(completed[ii], "completed[" + ii + "]");
            }
            // verify false for the next 50
            for (int ii = 50; ii < completed.length; ii++) {
                Assert.eqFalse(completed[ii], "completed[" + ii + "]");
            }
        }, null, null);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.resetForUnitTests(false, true, 0, 4, 10, 5);
        // verify the type is correct
        // verify true for the first 50
        // verify false for the next 50
        updateGraph.runWithinUnitTestCycle(() -> {
            final JobScheduler scheduler = new UpdateGraphJobScheduler(updateGraph);
            scheduler.iterateParallel(
                    ExecutionContext.getContext(),
                    null,
                    observer,
                    0,
                    50,
                    (context, idx, nec, resume) -> {
                        assertNotNull(context);
                        completed[idx] = true;
                        resume.run();
                    },
                    observer::onComplete,
                    observer::cleanup,
                    observer::onError);
        });
        observer.awaitFinished(Duration.ofSeconds(10));
        observer.assertSuccess();
        observer.assertNoOpenContexts();
    }

    @Test
    public void testSerialWithResume() throws InterruptedException, TimeoutException {
        final boolean[] completed = new boolean[100];
        final Observer observer = new Observer(() -> {
            // verify true for the first 50
            for (int ii = 0; ii < 50; ii++) {
                Assert.eqTrue(completed[ii], "completed[" + ii + "]");
            }
            // verify false for the next 50
            for (int ii = 50; ii < completed.length; ii++) {
                Assert.eqFalse(completed[ii], "completed[" + ii + "]");
            }
        }, null, null);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.resetForUnitTests(false, true, 0, 4, 10, 5);
        // verify true for the first 50
        // verify false for the next 50
        updateGraph.runWithinUnitTestCycle(() -> {
            final JobScheduler scheduler = new UpdateGraphJobScheduler(updateGraph);
            scheduler.iterateSerial(
                    ExecutionContext.getContext(),
                    null,
                    JobScheduler.DEFAULT_CONTEXT_FACTORY,
                    0,
                    50,
                    (context, idx, nec, resume) -> {
                        completed[idx] = true;
                        resume.run();
                    },
                    observer::onComplete,
                    observer::cleanup,
                    observer::onError);
        });
        observer.awaitFinished(Duration.ofSeconds(10));
        observer.assertSuccess();
    }

    @Test
    public void testSerialWithContext() throws InterruptedException, TimeoutException {
        final boolean[] completed = new boolean[100];
        final Observer observer = new Observer(() -> {
            // verify true for the first 50
            for (int ii = 0; ii < 50; ii++) {
                Assert.eqTrue(completed[ii], "completed[" + ii + "]");
            }
            // verify false for the next 50
            for (int ii = 50; ii < completed.length; ii++) {
                Assert.eqFalse(completed[ii], "completed[" + ii + "]");
            }
        }, null, null);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.resetForUnitTests(false, true, 0, 4, 10, 5);
        // verify the type is correct
        // verify true for the first 50
        // verify false for the next 50
        updateGraph.runWithinUnitTestCycle(() -> {
            final JobScheduler scheduler = new UpdateGraphJobScheduler(updateGraph);
            scheduler.iterateSerial(
                    ExecutionContext.getContext(),
                    null,
                    observer,
                    0,
                    50,
                    (context, idx, nec, resume) -> {
                        assertNotNull(context);

                        completed[idx] = true;
                        resume.run();
                    },
                    observer::onComplete,
                    observer::cleanup,
                    observer::onError);
        });
        observer.awaitFinished(Duration.ofSeconds(10));
        observer.assertSuccess();
        observer.assertNoOpenContexts();
    }

    @Test
    public void testSerialEmpty() throws InterruptedException, TimeoutException {
        final Observer observer = new Observer(null, null, null);
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.resetForUnitTests(false, true, 0, 4, 10, 5);
        // nop
        updateGraph.runWithinUnitTestCycle(() -> {

            final JobScheduler scheduler = new UpdateGraphJobScheduler(updateGraph);
            scheduler.iterateSerial(
                    ExecutionContext.getContext(),
                    null,
                    JobScheduler.DEFAULT_CONTEXT_FACTORY,
                    0,
                    0,
                    (context, idx, nec, resume) -> {
                        // nop
                    },
                    observer::onComplete,
                    observer::cleanup,
                    observer::onError);
        });
        observer.awaitFinished(Duration.ofSeconds(10));
        observer.assertSuccess();
    }

    @Test
    public void testSerialEmptyWithOnCompleteError() throws InterruptedException, TimeoutException {
        final Observer observer = new Observer(() -> {
            throw new RuntimeException("oops");
        }, null, null);
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.resetForUnitTests(false, true, 0, 4, 10, 5);
        // nop
        updateGraph.runWithinUnitTestCycle(() -> {
            final JobScheduler scheduler = new UpdateGraphJobScheduler(updateGraph);
            scheduler.iterateSerial(
                    ExecutionContext.getContext(),
                    null,
                    JobScheduler.DEFAULT_CONTEXT_FACTORY,
                    0,
                    0,
                    (context, idx, nec, resume) -> {
                        // nop
                    },
                    observer::onComplete,
                    observer::cleanup,
                    observer::onError);
        });
        observer.awaitFinished(Duration.ofSeconds(10));
        Assert.eqTrue(observer.error().getMessage().contains("oops"), "oops");
    }

    @Test
    public void testParallelEmpty() throws InterruptedException, TimeoutException {
        final Observer observer = new Observer(null, null, null);
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.resetForUnitTests(false, true, 0, 4, 10, 5);
        // nop
        UpdateGraph updateGraph1 = ExecutionContext.getContext().getUpdateGraph();
        updateGraph1.<ControlledUpdateGraph>cast().runWithinUnitTestCycle(() -> {
            final JobScheduler scheduler = new UpdateGraphJobScheduler(updateGraph);
            scheduler.iterateParallel(
                    ExecutionContext.getContext(),
                    null,
                    JobScheduler.DEFAULT_CONTEXT_FACTORY,
                    0,
                    0,
                    (context, idx, resume) -> {
                        // nop
                    },
                    observer::onComplete,
                    observer::cleanup,
                    observer::onError);
        });
        observer.awaitFinished(Duration.ofSeconds(10));
        observer.assertSuccess();
    }

    @Test
    public void testParallelEmptyWithOnCompleteError() throws InterruptedException, TimeoutException {
        final Observer observer = new Observer(() -> {
            throw new RuntimeException("oops");
        }, null, null);
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.resetForUnitTests(false, true, 0, 4, 10, 5);
        // nop
        UpdateGraph updateGraph1 = ExecutionContext.getContext().getUpdateGraph();
        updateGraph1.<ControlledUpdateGraph>cast().runWithinUnitTestCycle(() -> {
            final JobScheduler scheduler = new UpdateGraphJobScheduler(updateGraph);
            scheduler.iterateParallel(
                    ExecutionContext.getContext(),
                    null,
                    JobScheduler.DEFAULT_CONTEXT_FACTORY,
                    0,
                    0,
                    (context, idx, resume) -> {
                        // nop
                    },
                    observer::onComplete,
                    observer::cleanup,
                    observer::onError);
        });
        observer.awaitFinished(Duration.ofSeconds(10));
        Assert.eqTrue(observer.error().getMessage().contains("oops"), "oops");
    }

    @Test
    public void testParallelError() throws InterruptedException, TimeoutException {
        final Observer observer = new Observer(null, null, null);
        final boolean[] completed = new boolean[50];

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.resetForUnitTests(false, true, 0, 4, 10, 5);
        // verify the type is correct
        // throw before "doing work" to make verification easy
        // if this is called, we failed the test
        updateGraph.runWithinUnitTestCycle(() -> {

            final JobScheduler scheduler = new UpdateGraphJobScheduler(updateGraph);
            scheduler.iterateParallel(
                    ExecutionContext.getContext(),
                    null,
                    observer,
                    0,
                    50,
                    (context, idx, nec) -> {
                        assertNotNull(context);

                        // throw before "doing work" to make verification easy
                        if (idx == 10) {
                            throw new IndexOutOfBoundsException("Test error");
                        }

                        completed[idx] = true;
                    },
                    observer::onComplete,
                    observer::cleanup,
                    observer::onError);
        });
        observer.awaitFinished(Duration.ofSeconds(10));
        observer.assertDidNotCallComplete();
        Assert.neqNull(observer.error(), "observer.error()");
        observer.assertNoOpenContexts();
        Assert.eqFalse(completed[10], "completed[10]");
    }

    @Test
    public void testSerialError() throws InterruptedException, TimeoutException {
        final Observer observer = new Observer(null, null, null);
        final boolean[] completed = new boolean[100];

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.resetForUnitTests(false, true, 0, 4, 10, 5);
        // verify the type is correct
        // throw after this is set to make verification easy
        // if this is called, we failed the test
        // assert that the job was terminated before all tasks were executed (one is still false)
        updateGraph.runWithinUnitTestCycle(() -> {
            final JobScheduler scheduler = new UpdateGraphJobScheduler(updateGraph);
            scheduler.iterateSerial(
                    ExecutionContext.getContext(),
                    null,
                    observer,
                    0,
                    50,
                    (context, idx, nec, resume) -> {
                        assertNotNull(context);

                        completed[idx] = true;

                        // throw after this is set to make verification easy
                        if (idx == 10) {
                            throw new IndexOutOfBoundsException("test error");
                        }
                        resume.run();
                    },
                    observer::onComplete,
                    observer::cleanup,
                    observer::onError);
        });
        observer.awaitFinished(Duration.ofSeconds(10));
        observer.assertDidNotCallComplete();
        Assert.neqNull(observer.error(), "observer.error()");
        observer.assertNoOpenContexts();
        for (int i = 0; i < 100; ++i) {
            if (i <= 10) {
                Assert.eqTrue(completed[i], "completed[i]");
            } else {
                Assert.eqFalse(completed[i], "completed[i]");
            }
        }
    }

    @Test
    public void testNestedParallelError() throws InterruptedException, TimeoutException {
        final Observer observer = new Observer(null, null, null);
        final AtomicInteger openCount = new AtomicInteger(0);
        final boolean[][] completed = new boolean[50][60];

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.resetForUnitTests(false, true, 0, 4, 10, 5);
        // verify the type is correct
        // throw before "doing work" to make verification easy
        // if this is called, we failed the test
        updateGraph.runWithinUnitTestCycle(() -> {
            final JobScheduler scheduler = new UpdateGraphJobScheduler(updateGraph);
            scheduler.iterateParallel(
                    ExecutionContext.getContext(),
                    null,
                    observer,
                    0,
                    50,
                    (context1, idx1, nec1, r1) -> {
                        scheduler.iterateParallel(
                                ExecutionContext.getContext(),
                                null,
                                observer,
                                0,
                                60,
                                (context2, idx2, nec2) -> {
                                    assertNotNull(context2);

                                    // throw before "doing work" to make verification easy
                                    if (idx1 == 10 && idx2 == 10) {
                                        throw new IndexOutOfBoundsException("Test error");
                                    }

                                    completed[idx1][idx2] = true;
                                }, r1, () -> {
                                }, nec1);
                    },
                    observer::onComplete,
                    observer::cleanup,
                    observer::onError);
        });
        observer.awaitFinished(Duration.ofSeconds(10));
        observer.assertDidNotCallComplete();
        Assert.neqNull(observer.error(), "observer.error()");
        observer.assertNoOpenContexts();
        Assert.eqFalse(completed[10][10], "completed[10][10]");
    }

    @Test
    public void testNestedParallelChainedError() throws InterruptedException, TimeoutException {
        final Observer observer = new Observer(null, null, null);
        final boolean[][] completed = new boolean[50][60];

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.resetForUnitTests(false, true, 0, 4, 10, 5);
        // verify the type is correct
        // if this is called, we failed the test
        updateGraph.runWithinUnitTestCycle(() -> {
            final JobScheduler scheduler = new UpdateGraphJobScheduler(updateGraph);
            scheduler.iterateParallel(
                    ExecutionContext.getContext(),
                    null,
                    observer,
                    0,
                    50,
                    (context1, idx1, nec1, r1) -> {
                        if (idx1 == 40) {
                            throw new IndexOutOfBoundsException("Test error");
                        }
                        scheduler.iterateParallel(
                                ExecutionContext.getContext(),
                                null,
                                observer,
                                0,
                                60,
                                (context2, idx2, nec2) -> {
                                    assertNotNull(context2);
                                    completed[idx1][idx2] = true;
                                }, r1, () -> {
                                }, nec1);
                    },
                    observer::onComplete,
                    observer::cleanup,
                    observer::onError);
        });
        observer.awaitFinished(Duration.ofSeconds(10));
        observer.assertDidNotCallComplete();
        Assert.neqNull(observer.error(), "observer.error()");
        observer.assertNoOpenContexts();
        Assert.eqFalse(completed[40][0], "completed[10][10]");
    }

    @Test
    public void testNestedParallelChainedOnCompleteError() throws InterruptedException, TimeoutException {
        final Observer observer = new Observer(null, null, null);
        final boolean[][] completed = new boolean[50][60];

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.resetForUnitTests(false, true, 0, 4, 10, 5);
        // verify the type is correct
        updateGraph.runWithinUnitTestCycle(() -> {
            final JobScheduler scheduler = new UpdateGraphJobScheduler(updateGraph);
            scheduler.iterateParallel(
                    ExecutionContext.getContext(),
                    null,
                    observer,
                    0,
                    50,
                    (context1, idx1, nec1, r1) -> {
                        scheduler.iterateParallel(
                                ExecutionContext.getContext(),
                                null,
                                observer,
                                0,
                                60,
                                (context2, idx2, nec2) -> {
                                    assertNotNull(context2);
                                    completed[idx1][idx2] = true;
                                }, r1, () -> {
                                }, nec1);
                    },
                    observer::onComplete,
                    observer::cleanup,
                    observer::onError);
        });
        observer.awaitFinished(Duration.ofSeconds(10));
        observer.assertSuccess();
        observer.assertNoOpenContexts();
        for (int ii = 0; ii < 50; ++ii) {
            for (int jj = 0; jj < 60; ++jj) {
                Assert.eqTrue(completed[ii][jj], "completed[ii][jj]");
            }
        }
    }

    @Test
    public void testParallelErrorError() {
        final Observer observer = new Observer(null, null, exception -> {
            throw new IllegalStateException("Intentional error failure");
        });

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.resetForUnitTests(false, true, 0, 4, 10, 5);

        try {
            updateGraph.runWithinUnitTestCycle(() -> {
                final boolean[] completed = new boolean[50];

                final JobScheduler scheduler = new ImmediateJobScheduler();
                scheduler.iterateParallel(
                        ExecutionContext.getContext(),
                        null,
                        observer,
                        0,
                        50,
                        (context, idx, nec) -> {
                            assertNotNull(context);

                            // throw before "doing work" to make verification easy
                            if (idx == 10) {
                                throw new IndexOutOfBoundsException("Test error");
                            }

                            completed[idx] = true;
                        },
                        observer::onComplete,
                        observer::cleanup,
                        observer::onError);
            });
            TestCase.fail("Expected exception");
        } catch (FakeProcessEnvironment.FakeFatalException expected) {
            TestCase.assertEquals("Intentional error failure", expected.getCause().getMessage());
        }
    }

    @Test
    public void testParallelCompleteErrorError() {
        final Observer observer = new Observer(
                () -> {
                    throw new IllegalStateException("Intentional completion failure");
                },
                null,
                exception -> {
                    throw new IllegalStateException("Intentional error failure");
                });

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.resetForUnitTests(false, true, 0, 4, 10, 5);

        try {
            updateGraph.runWithinUnitTestCycle(() -> {
                final boolean[] completed = new boolean[50];

                final JobScheduler scheduler = new ImmediateJobScheduler();
                scheduler.iterateParallel(
                        ExecutionContext.getContext(),
                        null,
                        observer,
                        0,
                        50,
                        (context, idx, nec) -> {
                            assertNotNull(context);
                            completed[idx] = true;
                        },
                        observer::onComplete,
                        observer::cleanup,
                        observer::onError);
            });
            TestCase.fail("Expected exception");
        } catch (FakeProcessEnvironment.FakeFatalException expected) {
            // This actually goes through the FakeFatalErrorReporter twice; that's an artifact of the test design
            TestCase.assertEquals("Intentional error failure", expected.getCause().getMessage());
        }
    }

    private static class Observer extends ContextFactory {

        private final Runnable onComplete;
        private final Runnable cleanup;
        private final Consumer<Exception> onError;

        private final AtomicBoolean onCompleteInvoked;
        private final AtomicBoolean cleanupInvoked;
        private final AtomicReference<Exception> onErrorInvoked;
        private final CountDownLatch finished;

        public Observer(Runnable onComplete, Runnable cleanup, Consumer<Exception> onError) {
            this.onComplete = onComplete;
            this.cleanup = cleanup;
            this.onError = onError;
            onCompleteInvoked = new AtomicBoolean(false);
            cleanupInvoked = new AtomicBoolean(false);
            onErrorInvoked = new AtomicReference<>();
            finished = new CountDownLatch(1);
        }

        public void onComplete() {
            if (!onCompleteInvoked.compareAndSet(false, true)) {
                throw new IllegalStateException("onComplete call more than once");
            }
            if (onComplete != null) {
                onComplete.run();
            }
        }

        public void cleanup() {
            if (!onCompleteInvoked.get()) {
                throw new IllegalStateException("cleanup called, but onComplete not called");
            }
            if (onErrorInvoked.get() != null) {
                throw new IllegalStateException("cleanup called, but onError has already been called");
            }
            if (!cleanupInvoked.compareAndSet(false, true)) {
                throw new IllegalStateException("cleanup called more than once");
            }
            try {
                if (cleanup != null) {
                    cleanup.run();
                }
            } finally {
                finished.countDown();
            }
        }

        public void onError(Exception e) {
            if (cleanupInvoked.get()) {
                throw new IllegalStateException("onError called, but cleanup has already been called");
            }
            if (!onErrorInvoked.compareAndSet(null, e)) {
                throw new IllegalStateException("onError called more than once");
            }
            try {
                if (onError != null) {
                    onError.accept(e);
                }
            } finally {
                finished.countDown();
            }
        }

        public void awaitFinished(Duration timeout) throws InterruptedException, TimeoutException {
            if (!finished.await(timeout.toNanos(), TimeUnit.NANOSECONDS)) {
                throw new TimeoutException();
            }
        }

        public void assertDidNotCallComplete() {
            Assert.eqZero(finished.getCount(), "finished.getCount");
            Assert.eqFalse(onCompleteInvoked.get(), "onCompleteInvoked.get()");
        }

        public void assertSuccess() {
            Assert.eqZero(finished.getCount(), "finished.getCount");
            Assert.eqNull(onErrorInvoked.get(), "onErrorInvoked.get()");
        }

        public Exception error() {
            return onErrorInvoked.get();
        }
    }

    private static class ContextFactory implements Supplier<JobScheduler.JobThreadContext> {
        private final AtomicInteger openCount;

        public ContextFactory() {
            openCount = new AtomicInteger(0);
        }

        public void assertNoOpenContexts() {
            Assert.eqZero(openCount.get(), "openCount.get()");
        }

        @Override
        public JobScheduler.JobThreadContext get() {
            final JobThreadContextImpl ctx = new JobThreadContextImpl();
            openCount.incrementAndGet();
            return ctx;
        }

        class JobThreadContextImpl implements JobScheduler.JobThreadContext {

            @Override
            public void close() {
                openCount.decrementAndGet();
            }
        }
    }
}
