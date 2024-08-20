//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.AssertionFailure;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.exceptions.CancellationException;
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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertNotNull;

public final class TestJobScheduler {

    @Rule
    public final EngineCleanup cleanup = new EngineCleanup();

    @Test
    public void testParallel() {
        final CompletableFuture<Void> waitForResult = new CompletableFuture<>();

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.resetForUnitTests(false, true, 0, 4, 10, 5);
        // verify the set for the first 50
        updateGraph.runWithinUnitTestCycle(() -> {
            final boolean[] completed = new boolean[100];

            final JobScheduler scheduler = new UpdateGraphJobScheduler(updateGraph);
            scheduler.iterateParallel(
                    ExecutionContext.getContext(),
                    null,
                    JobScheduler.DEFAULT_CONTEXT_FACTORY,
                    0,
                    50,
                    (context, idx, nec) -> completed[idx] = true,
                    () -> {
                        // verify the set for the first 50
                        for (int ii = 0; ii < 50; ii++) {
                            Assert.eqTrue(completed[ii], "completed[" + ii + "]");
                        }
                        for (int ii = 50; ii < completed.length; ii++) {
                            Assert.eqFalse(completed[ii], "completed[" + ii + "]");
                        }
                        waitForResult.complete(null);
                    },
                    exception -> waitForResult.completeExceptionally(new AssertionFailure("unexpected error")));
        });

        try {
            // need to wait until this future is complete
            waitForResult.get();
        } catch (InterruptedException e) {
            throw new CancellationException("interrupted while processing test");
        } catch (ExecutionException e) {
            // rethrow the error
            throw new UncheckedDeephavenException("failure while processing test", e.getCause());
        }
    }

    @Test
    public void testParallelWithResume() {
        final CompletableFuture<Void> waitForResult = new CompletableFuture<>();

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.resetForUnitTests(false, true, 0, 4, 10, 5);
        // verify the set for the first 50
        updateGraph.runWithinUnitTestCycle(() -> {
            final boolean[] completed = new boolean[100];

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
                    () -> {
                        // verify the set for the first 50
                        for (int ii = 0; ii < 50; ii++) {
                            Assert.eqTrue(completed[ii], "completed[" + ii + "]");
                        }
                        for (int ii = 50; ii < completed.length; ii++) {
                            Assert.eqFalse(completed[ii], "completed[" + ii + "]");
                        }
                        waitForResult.complete(null);
                    },
                    exception -> waitForResult.completeExceptionally(new AssertionFailure("unexpected error")));
        });

        try {
            // need to wait until this future is complete
            waitForResult.get();
        } catch (InterruptedException e) {
            throw new CancellationException("interrupted while processing test");
        } catch (ExecutionException e) {
            // rethrow the error
            throw new UncheckedDeephavenException("failure while processing test", e.getCause());
        }
    }

    @Test
    public void testParallelWithContext() {
        final CompletableFuture<Void> waitForResult = new CompletableFuture<>();
        final AtomicInteger openCount = new AtomicInteger(0);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.resetForUnitTests(false, true, 0, 4, 10, 5);
        // verify the type is correct
        // verify true for the first 50
        // verify false for the next 50
        updateGraph.runWithinUnitTestCycle(() -> {

            class TestJobThreadContext implements JobScheduler.JobThreadContext {
                TestJobThreadContext() {
                    openCount.incrementAndGet();
                }

                @Override
                public void close() {
                    openCount.decrementAndGet();
                }
            }

            final boolean[] completed = new boolean[100];

            final JobScheduler scheduler = new UpdateGraphJobScheduler(updateGraph);
            scheduler.iterateParallel(
                    ExecutionContext.getContext(),
                    null,
                    TestJobThreadContext::new,
                    0,
                    50,
                    (context, idx, nec, resume) -> {
                        assertNotNull(context);

                        completed[idx] = true;
                        resume.run();
                    },
                    () -> {
                        // verify true for the first 50
                        for (int ii = 0; ii < 50; ii++) {
                            Assert.eqTrue(completed[ii], "completed[" + ii + "]");
                        }
                        // verify false for the next 50
                        for (int ii = 50; ii < completed.length; ii++) {
                            Assert.eqFalse(completed[ii], "completed[" + ii + "]");
                        }
                        waitForResult.complete(null);
                    },
                    exception -> waitForResult.completeExceptionally(new AssertionFailure("unexpected error")));
        });

        try {
            // need to wait until this future is complete
            waitForResult.get();
            // make sure all the contexts were closed
            Assert.eqZero(openCount.get(), "openCount");
        } catch (InterruptedException e) {
            throw new CancellationException("interrupted while processing test");
        } catch (ExecutionException e) {
            // rethrow the error
            throw new UncheckedDeephavenException("failure while processing test", e.getCause());
        }
    }

    @Test
    public void testSerialWithResume() {
        final CompletableFuture<Void> waitForResult = new CompletableFuture<>();

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.resetForUnitTests(false, true, 0, 4, 10, 5);
        // verify true for the first 50
        // verify false for the next 50
        updateGraph.runWithinUnitTestCycle(() -> {
            final boolean[] completed = new boolean[100];

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
                    () -> {
                        // verify true for the first 50
                        for (int ii = 0; ii < 50; ii++) {
                            Assert.eqTrue(completed[ii], "completed[" + ii + "]");
                        }
                        // verify false for the next 50
                        for (int ii = 50; ii < completed.length; ii++) {
                            Assert.eqFalse(completed[ii], "completed[" + ii + "]");
                        }
                        waitForResult.complete(null);
                    },
                    exception -> waitForResult.completeExceptionally(new AssertionFailure("unexpected error")));
        });

        try {
            // need to wait until this future is complete
            waitForResult.get();
        } catch (InterruptedException e) {
            throw new CancellationException("interrupted while processing test");
        } catch (ExecutionException e) {
            // rethrow the error
            throw new UncheckedDeephavenException("failure while processing test", e.getCause());
        }
    }

    @Test
    public void testSerialWithContext() {
        final CompletableFuture<Void> waitForResult = new CompletableFuture<>();
        final AtomicInteger openCount = new AtomicInteger(0);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.resetForUnitTests(false, true, 0, 4, 10, 5);
        // verify the type is correct
        // verify true for the first 50
        // verify false for the next 50
        updateGraph.runWithinUnitTestCycle(() -> {

            class TestJobThreadContext implements JobScheduler.JobThreadContext {
                TestJobThreadContext() {
                    openCount.incrementAndGet();
                }

                @Override
                public void close() {
                    openCount.decrementAndGet();
                }
            }

            final boolean[] completed = new boolean[100];

            final JobScheduler scheduler = new UpdateGraphJobScheduler(updateGraph);
            scheduler.iterateSerial(
                    ExecutionContext.getContext(),
                    null,
                    TestJobThreadContext::new,
                    0,
                    50,
                    (context, idx, nec, resume) -> {
                        assertNotNull(context);

                        completed[idx] = true;
                        resume.run();
                    },
                    () -> {
                        // verify true for the first 50
                        for (int ii = 0; ii < 50; ii++) {
                            Assert.eqTrue(completed[ii], "completed[" + ii + "]");
                        }
                        // verify false for the next 50
                        for (int ii = 50; ii < completed.length; ii++) {
                            Assert.eqFalse(completed[ii], "completed[" + ii + "]");
                        }
                        waitForResult.complete(null);
                    },
                    exception -> waitForResult.completeExceptionally(new AssertionFailure("unexpected error")));
        });

        try {
            // need to wait until this future is complete
            waitForResult.get();
            // make sure all the contexts were closed
            Assert.eqZero(openCount.get(), "openCount");
        } catch (InterruptedException e) {
            throw new CancellationException("interrupted while processing test");
        } catch (ExecutionException e) {
            // rethrow the error
            throw new UncheckedDeephavenException("failure while processing test", e.getCause());
        }
    }

    @Test
    public void testSerialEmpty() {
        final CompletableFuture<Void> waitForResult = new CompletableFuture<>();

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
                    () -> waitForResult.complete(null),
                    exception -> waitForResult.completeExceptionally(new AssertionFailure("unexpected error")));
        });

        try {
            // need to wait until this future is complete
            waitForResult.get();
        } catch (InterruptedException e) {
            throw new CancellationException("interrupted while processing test");
        } catch (ExecutionException e) {
            // rethrow the error
            throw new UncheckedDeephavenException("failure while processing test", e.getCause());
        }
    }

    @Test
    public void testParallelEmpty() {
        final CompletableFuture<Void> waitForResult = new CompletableFuture<>();

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
                    () -> waitForResult.complete(null),
                    exception -> waitForResult.completeExceptionally(new AssertionFailure("unexpected error")));
        });

        try {
            // need to wait until this future is complete
            waitForResult.get();
        } catch (InterruptedException e) {
            throw new CancellationException("interrupted while processing test");
        } catch (ExecutionException e) {
            // rethrow the error
            throw new UncheckedDeephavenException("failure while processing test", e.getCause());
        }
    }

    @Test
    public void testParallelError() {
        final CompletableFuture<Void> waitForResult = new CompletableFuture<>();
        final AtomicInteger openCount = new AtomicInteger(0);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.resetForUnitTests(false, true, 0, 4, 10, 5);
        // verify the type is correct
        // throw before "doing work" to make verification easy
        // if this is called, we failed the test
        updateGraph.runWithinUnitTestCycle(() -> {
            final boolean[] completed = new boolean[50];

            class TestJobThreadContext implements JobScheduler.JobThreadContext {
                TestJobThreadContext() {
                    openCount.incrementAndGet();
                }

                @Override
                public void close() {
                    openCount.decrementAndGet();
                }
            }

            final JobScheduler scheduler = new UpdateGraphJobScheduler(updateGraph);
            scheduler.iterateParallel(
                    ExecutionContext.getContext(),
                    null,
                    TestJobThreadContext::new,
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
                    () -> {
                        // if this is called, we failed the test
                        waitForResult.completeExceptionally(new AssertionFailure("Exception not thrown"));
                    },
                    exception -> {
                        if (!(exception instanceof IndexOutOfBoundsException)) {
                            waitForResult.completeExceptionally(new AssertionFailure("Unexpected exception thrown"));
                        }
                        if (completed[10]) {
                            waitForResult.completeExceptionally(new AssertionFailure("Processed unexpected index"));
                        }
                        waitForResult.complete(null);
                    });
        });

        try {
            // need to wait until this future is complete
            waitForResult.get();
            // make sure all the contexts were closed
            Assert.eqZero(openCount.get(), "openCount");
        } catch (InterruptedException e) {
            throw new CancellationException("Interrupted while processing test");
        } catch (ExecutionException e) {
            // rethrow the error
            throw new UncheckedDeephavenException("Failure while processing test", e.getCause());
        }
    }

    @Test
    public void testSerialError() {
        final CompletableFuture<Void> waitForResult = new CompletableFuture<>();
        final AtomicInteger openCount = new AtomicInteger(0);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.resetForUnitTests(false, true, 0, 4, 10, 5);
        // verify the type is correct
        // throw after this is set to make verification easy
        // if this is called, we failed the test
        // assert that the job was terminated before all tasks were executed (one is still false)
        updateGraph.runWithinUnitTestCycle(() -> {
            final boolean[] completed = new boolean[100];

            class TestJobThreadContext implements JobScheduler.JobThreadContext {
                TestJobThreadContext() {
                    openCount.incrementAndGet();
                }

                @Override
                public void close() {
                    openCount.decrementAndGet();
                }
            }

            final JobScheduler scheduler = new UpdateGraphJobScheduler(updateGraph);
            scheduler.iterateSerial(
                    ExecutionContext.getContext(),
                    null,
                    TestJobThreadContext::new,
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
                    () -> {
                        // if this is called, we failed the test
                        waitForResult
                                .completeExceptionally(new AssertionFailure("IndexOutOfBoundsException not thrown"));
                    },
                    exception -> {
                        if (!(exception instanceof IndexOutOfBoundsException)) {
                            waitForResult.completeExceptionally(
                                    new AssertionFailure("IndexOutOfBoundsException not thrown"));
                        }

                        // assert that the job was terminated before all tasks were executed (one is still false)
                        for (int ii = 0; ii < 50; ii++) {
                            if (!completed[ii]) {
                                waitForResult.complete(null);
                                return;
                            }
                        }
                        waitForResult.completeExceptionally(new AssertionFailure("Tasks not terminated"));
                    });
        });

        try {
            // need to wait until this future is complete
            waitForResult.get();
            // make sure all the contexts were closed
            Assert.eqZero(openCount.get(), "openCount");
        } catch (InterruptedException e) {
            throw new CancellationException("interrupted while processing test");
        } catch (ExecutionException e) {
            // rethrow the error
            throw new UncheckedDeephavenException("failure while processing test", e.getCause());
        }
    }

    @Test
    public void testNestedParallelError() {
        final CompletableFuture<Void> waitForResult = new CompletableFuture<>();
        final AtomicInteger openCount = new AtomicInteger(0);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.resetForUnitTests(false, true, 0, 4, 10, 5);
        // verify the type is correct
        // throw before "doing work" to make verification easy
        // if this is called, we failed the test
        updateGraph.runWithinUnitTestCycle(() -> {
            final boolean[][] completed = new boolean[50][60];

            class TestJobThreadContext implements JobScheduler.JobThreadContext {
                TestJobThreadContext() {
                    openCount.incrementAndGet();
                }

                @Override
                public void close() {
                    openCount.decrementAndGet();
                }
            }

            final JobScheduler scheduler = new UpdateGraphJobScheduler(updateGraph);
            scheduler.iterateParallel(
                    ExecutionContext.getContext(),
                    null,
                    TestJobThreadContext::new,
                    0,
                    50,
                    (context1, idx1, nec1, r1) -> {
                        scheduler.iterateParallel(
                                ExecutionContext.getContext(),
                                null,
                                TestJobThreadContext::new,
                                0,
                                60,
                                (context2, idx2, nec2) -> {
                                    assertNotNull(context2);

                                    // throw before "doing work" to make verification easy
                                    if (idx1 == 10 && idx2 == 10) {
                                        throw new IndexOutOfBoundsException("Test error");
                                    }

                                    completed[idx1][idx2] = true;
                                }, r1, nec1);
                    },
                    () -> {
                        // if this is called, we failed the test
                        waitForResult.completeExceptionally(new AssertionFailure("Exception not thrown"));
                    },
                    exception -> {
                        if (!(exception instanceof IndexOutOfBoundsException)) {
                            waitForResult.completeExceptionally(new AssertionFailure("Unexpected exception thrown"));
                        }
                        if (completed[10][10]) {
                            waitForResult.completeExceptionally(new AssertionFailure("Processed unexpected index"));
                        }
                        waitForResult.complete(null);
                    });
        });

        try {
            // need to wait until this future is complete
            waitForResult.get();
            // make sure all the contexts were closed
            Assert.eqZero(openCount.get(), "openCount");
        } catch (InterruptedException e) {
            throw new CancellationException("Interrupted while processing test");
        } catch (ExecutionException e) {
            // rethrow the error
            throw new UncheckedDeephavenException("Failure while processing test", e.getCause());
        }
    }

    @Test
    public void testNestedParallelChainedError() {
        final CompletableFuture<Void> waitForResult = new CompletableFuture<>();
        final AtomicInteger openCount = new AtomicInteger(0);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.resetForUnitTests(false, true, 0, 4, 10, 5);
        // verify the type is correct
        // if this is called, we failed the test
        updateGraph.runWithinUnitTestCycle(() -> {
            final boolean[][] completed = new boolean[50][60];

            class TestJobThreadContext implements JobScheduler.JobThreadContext {
                TestJobThreadContext() {
                    openCount.incrementAndGet();
                }

                @Override
                public void close() {
                    openCount.decrementAndGet();
                }
            }

            final JobScheduler scheduler = new UpdateGraphJobScheduler(updateGraph);
            scheduler.iterateParallel(
                    ExecutionContext.getContext(),
                    null,
                    TestJobThreadContext::new,
                    0,
                    50,
                    (context1, idx1, nec1, r1) -> {
                        if (idx1 == 40) {
                            throw new IndexOutOfBoundsException("Test error");
                        }
                        scheduler.iterateParallel(
                                ExecutionContext.getContext(),
                                null,
                                TestJobThreadContext::new,
                                0,
                                60,
                                (context2, idx2, nec2) -> {
                                    assertNotNull(context2);
                                    completed[idx1][idx2] = true;
                                }, r1, nec1);
                    },
                    () -> {
                        // if this is called, we failed the test
                        waitForResult.completeExceptionally(new AssertionFailure("Exception not thrown"));
                    },
                    exception -> {
                        if (!(exception instanceof IndexOutOfBoundsException)) {
                            waitForResult.completeExceptionally(new AssertionFailure("Unexpected exception thrown"));
                        }
                        if (completed[40][0]) {
                            waitForResult.completeExceptionally(new AssertionFailure("Processed unexpected index"));
                        }
                        waitForResult.complete(null);
                    });
        });

        try {
            // need to wait until this future is complete
            waitForResult.get();
            // make sure all the contexts were closed
            Assert.eqZero(openCount.get(), "openCount");
        } catch (InterruptedException e) {
            throw new CancellationException("Interrupted while processing test");
        } catch (ExecutionException e) {
            // rethrow the error
            throw new UncheckedDeephavenException("Failure while processing test", e.getCause());
        }
    }

    @Test
    public void testNestedParallelChainedOnCompleteError() {
        final CompletableFuture<Void> waitForResult = new CompletableFuture<>();
        final AtomicInteger openCount = new AtomicInteger(0);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.resetForUnitTests(false, true, 0, 4, 10, 5);
        // verify the type is correct
        updateGraph.runWithinUnitTestCycle(() -> {
            final boolean[][] completed = new boolean[50][60];

            class TestJobThreadContext implements JobScheduler.JobThreadContext {
                TestJobThreadContext() {
                    openCount.incrementAndGet();
                }

                @Override
                public void close() {
                    openCount.decrementAndGet();
                }
            }

            final JobScheduler scheduler = new UpdateGraphJobScheduler(updateGraph);
            scheduler.iterateParallel(
                    ExecutionContext.getContext(),
                    null,
                    TestJobThreadContext::new,
                    0,
                    50,
                    (context1, idx1, nec1, r1) -> {
                        scheduler.iterateParallel(
                                ExecutionContext.getContext(),
                                null,
                                TestJobThreadContext::new,
                                0,
                                60,
                                (context2, idx2, nec2) -> {
                                    assertNotNull(context2);
                                    completed[idx1][idx2] = true;
                                }, r1, nec1);
                    },
                    () -> {
                        throw new IllegalStateException("Intentional completion failure");
                    },
                    exception -> {
                        if (!(exception instanceof IllegalStateException)) {
                            waitForResult.completeExceptionally(new AssertionFailure("Unexpected exception thrown"));
                        }
                        for (int ii = 0; ii < 50; ++ii) {
                            for (int jj = 0; jj < 60; ++jj) {
                                if (!completed[ii][jj]) {
                                    waitForResult.completeExceptionally(new AssertionFailure(
                                            String.format("Failed to process index [%d][%d]", ii, jj)));
                                }
                            }
                        }
                        waitForResult.complete(null);
                    });
        });

        try {
            // need to wait until this future is complete
            waitForResult.get();
            // make sure all the contexts were closed
            Assert.eqZero(openCount.get(), "openCount");
        } catch (InterruptedException e) {
            throw new CancellationException("Interrupted while processing test");
        } catch (ExecutionException e) {
            // rethrow the error
            throw new UncheckedDeephavenException("Failure while processing test", e.getCause());
        }
    }

    @Test
    public void testParallelErrorError() {
        final CompletableFuture<Void> waitForResult = new CompletableFuture<>();
        final AtomicInteger openCount = new AtomicInteger(0);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.resetForUnitTests(false, true, 0, 4, 10, 5);

        try {
            updateGraph.runWithinUnitTestCycle(() -> {
                final boolean[] completed = new boolean[50];

                class TestJobThreadContext implements JobScheduler.JobThreadContext {
                    TestJobThreadContext() {
                        openCount.incrementAndGet();
                    }

                    @Override
                    public void close() {
                        openCount.decrementAndGet();
                    }
                }

                final JobScheduler scheduler = new ImmediateJobScheduler();
                scheduler.iterateParallel(
                        ExecutionContext.getContext(),
                        null,
                        TestJobThreadContext::new,
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
                        () -> {
                            // if this is called, we failed the test
                            waitForResult.completeExceptionally(new AssertionFailure("Exception not thrown"));
                        },
                        exception -> {
                            throw new IllegalStateException("Intentional error failure");
                        });
            });
            TestCase.fail("Expected exception");
        } catch (FakeProcessEnvironment.FakeFatalException expected) {
            TestCase.assertEquals("Intentional error failure", expected.getCause().getCause().getMessage());
        }
    }

    @Test
    public void testParallelCompleteErrorError() {
        final CompletableFuture<Void> waitForResult = new CompletableFuture<>();
        final AtomicInteger openCount = new AtomicInteger(0);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.resetForUnitTests(false, true, 0, 4, 10, 5);

        try {
            updateGraph.runWithinUnitTestCycle(() -> {
                final boolean[] completed = new boolean[50];

                class TestJobThreadContext implements JobScheduler.JobThreadContext {
                    TestJobThreadContext() {
                        openCount.incrementAndGet();
                    }

                    @Override
                    public void close() {
                        openCount.decrementAndGet();
                    }
                }

                final JobScheduler scheduler = new ImmediateJobScheduler();
                scheduler.iterateParallel(
                        ExecutionContext.getContext(),
                        null,
                        TestJobThreadContext::new,
                        0,
                        50,
                        (context, idx, nec) -> {
                            assertNotNull(context);
                            completed[idx] = true;
                        },
                        () -> {
                            throw new IllegalStateException("Intentional completion failure");
                        },
                        exception -> {
                            throw new IllegalStateException("Intentional error failure");
                        });
            });
            TestCase.fail("Expected exception");
        } catch (FakeProcessEnvironment.FakeFatalException expected) {
            // This actually goes through the FakeFatalErrorReporter twice; that's an artifact of the test design
            TestCase.assertEquals("Intentional error failure", expected.getCause().getCause().getCause().getMessage());
        }
    }
}
