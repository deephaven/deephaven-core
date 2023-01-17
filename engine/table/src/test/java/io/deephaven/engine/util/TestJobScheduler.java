package io.deephaven.engine.util;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.AssertionFailure;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.exceptions.CancellationException;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.engine.table.impl.util.UpdateGraphProcessorJobScheduler;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.test.types.OutOfBandTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

@Category(OutOfBandTest.class)
public final class TestJobScheduler {

    @Test
    public void TestParallel() {
        final CompletableFuture<Void> waitForResult = new CompletableFuture<>();

        UpdateGraphProcessor.DEFAULT.enableUnitTestMode();
        UpdateGraphProcessor.DEFAULT.resetForUnitTests(false, true, 0, 4, 10, 5);
        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            final boolean[] completed = new boolean[100];

            final JobScheduler scheduler = new UpdateGraphProcessorJobScheduler();
            scheduler.iterateParallel(
                    ExecutionContext.getContext(),
                    null,
                    JobScheduler.JobContext::new,
                    0,
                    50,
                    (context, idx) -> {
                        completed[idx] = true;
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
                    exception -> {
                        waitForResult.completeExceptionally(new AssertionFailure("unexpected error"));
                    });
        });

        try {
            // need to wait until this future is complete
            waitForResult.get();
        } catch (InterruptedException e) {
            throw new CancellationException("interrupted while processing test");
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else {
                // rethrow the error
                throw new UncheckedDeephavenException("failure while processing test", e.getCause());
            }
        }
    }

    @Test
    public void TestParallelWithResume() {
        final CompletableFuture<Void> waitForResult = new CompletableFuture<>();

        UpdateGraphProcessor.DEFAULT.enableUnitTestMode();
        UpdateGraphProcessor.DEFAULT.resetForUnitTests(false, true, 0, 4, 10, 5);
        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            final boolean[] completed = new boolean[100];

            final JobScheduler scheduler = new UpdateGraphProcessorJobScheduler();
            scheduler.iterateParallel(
                    ExecutionContext.getContext(),
                    null,
                    JobScheduler.JobContext::new,
                    0,
                    50,
                    (context, idx, resume) -> {
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
                    exception -> {
                        waitForResult.completeExceptionally(new AssertionFailure("unexpected error"));
                    });
        });

        try {
            // need to wait until this future is complete
            waitForResult.get();
        } catch (InterruptedException e) {
            throw new CancellationException("interrupted while processing test");
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else {
                // rethrow the error
                throw new UncheckedDeephavenException("failure while processing test", e.getCause());
            }
        }
    }

    @Test
    public void TestParallelWithContext() {
        final CompletableFuture<Void> waitForResult = new CompletableFuture<>();
        final AtomicInteger openCount = new AtomicInteger(0);

        UpdateGraphProcessor.DEFAULT.enableUnitTestMode();
        UpdateGraphProcessor.DEFAULT.resetForUnitTests(false, true, 0, 4, 10, 5);
        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {

            class TestJobContext extends JobScheduler.JobContext {
                TestJobContext() {
                    openCount.incrementAndGet();
                }

                @Override
                public void close() {
                    openCount.decrementAndGet();
                }
            }

            final boolean[] completed = new boolean[100];

            final JobScheduler scheduler = new UpdateGraphProcessorJobScheduler();
            scheduler.iterateParallel(
                    ExecutionContext.getContext(),
                    null,
                    TestJobContext::new,
                    0,
                    50,
                    (context, idx, resume) -> {
                        // verify the type is correct
                        Assert.instanceOf(context, "context", TestJobContext.class);

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
                    exception -> {
                        waitForResult.completeExceptionally(new AssertionFailure("unexpected error"));
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
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else {
                // rethrow the error
                throw new UncheckedDeephavenException("failure while processing test", e.getCause());
            }
        }
    }

    @Test
    public void TestSerialWithResume() {
        final CompletableFuture<Void> waitForResult = new CompletableFuture<>();

        UpdateGraphProcessor.DEFAULT.enableUnitTestMode();
        UpdateGraphProcessor.DEFAULT.resetForUnitTests(false, true, 0, 4, 10, 5);
        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            final boolean[] completed = new boolean[100];

            final JobScheduler scheduler = new UpdateGraphProcessorJobScheduler();
            scheduler.iterateSerial(
                    ExecutionContext.getContext(),
                    null,
                    JobScheduler.JobContext::new,
                    0,
                    50,
                    (context, idx, resume) -> {
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
                    exception -> {
                        waitForResult.completeExceptionally(new AssertionFailure("unexpected error"));
                    });
        });

        try {
            // need to wait until this future is complete
            waitForResult.get();
        } catch (InterruptedException e) {
            throw new CancellationException("interrupted while processing test");
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else {
                // rethrow the error
                throw new UncheckedDeephavenException("failure while processing test", e.getCause());
            }
        }
    }

    @Test
    public void TestSerialWithContext() {
        final CompletableFuture<Void> waitForResult = new CompletableFuture<>();
        final AtomicInteger openCount = new AtomicInteger(0);

        UpdateGraphProcessor.DEFAULT.enableUnitTestMode();
        UpdateGraphProcessor.DEFAULT.resetForUnitTests(false, true, 0, 4, 10, 5);
        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {

            class TestJobContext extends JobScheduler.JobContext {
                TestJobContext() {
                    openCount.incrementAndGet();
                }

                @Override
                public void close() {
                    openCount.decrementAndGet();
                }
            }

            final boolean[] completed = new boolean[100];

            final JobScheduler scheduler = new UpdateGraphProcessorJobScheduler();
            scheduler.iterateSerial(
                    ExecutionContext.getContext(),
                    null,
                    TestJobContext::new,
                    0,
                    50,
                    (context, idx, resume) -> {
                        // verify the type is correct
                        Assert.instanceOf(context, "context", TestJobContext.class);

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
                    exception -> {
                        waitForResult.completeExceptionally(new AssertionFailure("unexpected error"));
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
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else {
                // rethrow the error
                throw new UncheckedDeephavenException("failure while processing test", e.getCause());
            }
        }
    }

    @Test
    public void TestSerialEmpty() {
        final CompletableFuture<Void> waitForResult = new CompletableFuture<>();

        UpdateGraphProcessor.DEFAULT.enableUnitTestMode();
        UpdateGraphProcessor.DEFAULT.resetForUnitTests(false, true, 0, 4, 10, 5);
        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {

            final JobScheduler scheduler = new UpdateGraphProcessorJobScheduler();
            scheduler.iterateSerial(
                    ExecutionContext.getContext(),
                    null,
                    JobScheduler.JobContext::new,
                    0,
                    0,
                    (context, idx, resume) -> {
                        // nop
                    },
                    () -> {
                        waitForResult.complete(null);
                    },
                    exception -> {
                        waitForResult.completeExceptionally(new AssertionFailure("unexpected error"));
                    });
        });

        try {
            // need to wait until this future is complete
            waitForResult.get();
        } catch (InterruptedException e) {
            throw new CancellationException("interrupted while processing test");
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else {
                // rethrow the error
                throw new UncheckedDeephavenException("failure while processing test", e.getCause());
            }
        }
    }

    @Test
    public void TestParallelEmpty() {
        final CompletableFuture<Void> waitForResult = new CompletableFuture<>();

        UpdateGraphProcessor.DEFAULT.enableUnitTestMode();
        UpdateGraphProcessor.DEFAULT.resetForUnitTests(false, true, 0, 4, 10, 5);
        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {

            final JobScheduler scheduler = new UpdateGraphProcessorJobScheduler();
            scheduler.iterateParallel(
                    ExecutionContext.getContext(),
                    null,
                    JobScheduler.JobContext::new,
                    0,
                    0,
                    (context, idx, resume) -> {
                        // nop
                    },
                    () -> {
                        waitForResult.complete(null);
                    },
                    exception -> {
                        waitForResult.completeExceptionally(new AssertionFailure("unexpected error"));
                    });
        });

        try {
            // need to wait until this future is complete
            waitForResult.get();
        } catch (InterruptedException e) {
            throw new CancellationException("interrupted while processing test");
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else {
                // rethrow the error
                throw new UncheckedDeephavenException("failure while processing test", e.getCause());
            }
        }
    }

    @Test
    public void TestParallelError() {
        final CompletableFuture<Void> waitForResult = new CompletableFuture<>();
        final AtomicInteger openCount = new AtomicInteger(0);

        UpdateGraphProcessor.DEFAULT.enableUnitTestMode();
        UpdateGraphProcessor.DEFAULT.resetForUnitTests(false, true, 0, 4, 10, 5);
        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            final boolean[] completed = new boolean[100];

            class TestJobContext extends JobScheduler.JobContext {
                TestJobContext() {
                    openCount.incrementAndGet();
                }

                @Override
                public void close() {
                    openCount.decrementAndGet();
                }
            }

            final JobScheduler scheduler = new UpdateGraphProcessorJobScheduler();
            scheduler.iterateParallel(
                    ExecutionContext.getContext(),
                    null,
                    TestJobContext::new,
                    0,
                    50,
                    (context, idx) -> {
                        // verify the type is correct
                        Assert.instanceOf(context, "context", TestJobContext.class);

                        completed[idx] = true;
                        // throw after this is set to make verification easy
                        if (idx == 10) {
                            throw new IndexOutOfBoundsException("test error");
                        }
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
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else {
                // rethrow the error
                throw new UncheckedDeephavenException("failure while processing test", e.getCause());
            }
        }
    }

    @Test
    public void TestSerialError() {
        final CompletableFuture<Void> waitForResult = new CompletableFuture<>();
        final AtomicInteger openCount = new AtomicInteger(0);

        UpdateGraphProcessor.DEFAULT.enableUnitTestMode();
        UpdateGraphProcessor.DEFAULT.resetForUnitTests(false, true, 0, 4, 10, 5);
        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            final boolean[] completed = new boolean[100];

            class TestJobContext extends JobScheduler.JobContext {
                TestJobContext() {
                    openCount.incrementAndGet();
                }

                @Override
                public void close() {
                    openCount.decrementAndGet();
                }
            }

            final JobScheduler scheduler = new UpdateGraphProcessorJobScheduler();
            scheduler.iterateSerial(
                    ExecutionContext.getContext(),
                    null,
                    TestJobContext::new,
                    0,
                    50,
                    (context, idx, resume) -> {
                        // verify the type is correct
                        Assert.instanceOf(context, "context", TestJobContext.class);

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
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else {
                // rethrow the error
                throw new UncheckedDeephavenException("failure while processing test", e.getCause());
            }
        }
    }
}
