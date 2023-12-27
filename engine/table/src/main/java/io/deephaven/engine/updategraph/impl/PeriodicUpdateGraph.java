/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.updategraph.impl;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.SleepUtil;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.util.pools.MultiChunkPool;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.updategraph.*;
import io.deephaven.engine.util.systemicmarking.SystemicObjectTracker;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.TestUseOnly;
import io.deephaven.util.datastructures.linked.IntrusiveDoublyLinkedNode;
import io.deephaven.util.datastructures.linked.IntrusiveDoublyLinkedQueue;
import io.deephaven.util.function.ThrowingRunnable;
import io.deephaven.util.thread.NamingThreadFactory;
import io.deephaven.util.thread.ThreadInitializationFactory;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * <p>
 * This class uses a thread (or pool of threads) to periodically update a set of monitored update sources at a specified
 * target cycle interval. The target cycle interval can be {@link #setTargetCycleDurationMillis(long) configured} to
 * reduce or increase the run rate of the monitored sources.
 * <p>
 * This class can be configured via the following {@link Configuration} property
 * <ul>
 * <li>{@value DEFAULT_TARGET_CYCLE_DURATION_MILLIS_PROP}(optional) - The default target cycle time in ms (1000 if not
 * defined)</li>
 * </ul>
 */
public class PeriodicUpdateGraph extends BaseUpdateGraph {

    public static final int NUM_THREADS_DEFAULT_UPDATE_GRAPH =
            Configuration.getInstance().getIntegerWithDefault("PeriodicUpdateGraph.updateThreads", -1);

    public static Builder newBuilder(final String name) {
        return new Builder(name);
    }


    private static final Logger log = LoggerFactory.getLogger(PeriodicUpdateGraph.class);

    /**
     * A flag indicating that an accelerated cycle has been requested.
     */
    private final AtomicBoolean refreshRequested = new AtomicBoolean();

    /**
     * The core refresh driver thread, constructed and started during {@link #start()}.
     */
    private Thread refreshThread;

    /**
     * {@link ScheduledExecutorService} used for scheduling the {@link #watchDogTimeoutProcedure}, constructed during
     * {@link #start()}.
     */
    private ScheduledExecutorService watchdogScheduler;

    /**
     * If this is set to a positive value, then we will call the {@link #watchDogTimeoutProcedure} if any single run
     * loop takes longer than this value. The intention is to use this for strategies, or other queries, where a
     * PeriodicUpdateGraph loop that is "stuck" is the equivalent of an error. Set the value with
     * {@link #setWatchDogMillis(int)}.
     */
    private volatile int watchDogMillis = 0;
    /**
     * If a timeout time has been {@link #setWatchDogMillis(int) set}, this procedure will be called if any single run
     * loop takes longer than the value specified. Set the value with
     * {@link #setWatchDogTimeoutProcedure(LongConsumer)}.
     */
    private volatile LongConsumer watchDogTimeoutProcedure;

    public static final String ALLOW_UNIT_TEST_MODE_PROP = "PeriodicUpdateGraph.allowUnitTestMode";
    private final boolean allowUnitTestMode;
    private int notificationAdditionDelay;
    private Random notificationRandomizer = new Random(0);
    private boolean unitTestMode;
    private ExecutorService unitTestRefreshThreadPool;

    public static final String DEFAULT_TARGET_CYCLE_DURATION_MILLIS_PROP =
            "PeriodicUpdateGraph.targetCycleDurationMillis";
    private final long defaultTargetCycleDurationMillis;
    private volatile long targetCycleDurationMillis;
    private final ThreadInitializationFactory threadInitializationFactory;


    /**
     * The number of threads in our executor service for dispatching notifications. If 1, then we don't actually use the
     * executor service; but instead dispatch all the notifications on the PeriodicUpdateGraph run thread.
     */
    private final int updateThreads;

    private final long minimumInterCycleSleep =
            Configuration.getInstance().getIntegerWithDefault("PeriodicUpdateGraph.minimumInterCycleSleep", 0);
    private final boolean interCycleYield =
            Configuration.getInstance().getBooleanWithDefault("PeriodicUpdateGraph.interCycleYield", false);

    public PeriodicUpdateGraph(
            final String name,
            final boolean allowUnitTestMode,
            final long targetCycleDurationMillis,
            final long minimumCycleDurationToLogNanos,
            final int numUpdateThreads,
            final ThreadInitializationFactory threadInitializationFactory) {
        super(name, allowUnitTestMode, log, minimumCycleDurationToLogNanos);
        this.allowUnitTestMode = allowUnitTestMode;
        this.defaultTargetCycleDurationMillis = targetCycleDurationMillis;
        this.targetCycleDurationMillis = targetCycleDurationMillis;
        this.threadInitializationFactory = threadInitializationFactory;

        if (numUpdateThreads <= 0) {
            this.updateThreads = Runtime.getRuntime().availableProcessors();
        } else {
            this.updateThreads = numUpdateThreads;
        }
    }

    @Override
    public LogOutput append(@NotNull final LogOutput logOutput) {
        return logOutput.append("PeriodicUpdateGraph-").append(getName());
    }

    @NotNull
    private NotificationProcessor makeNotificationProcessor() {
        if (updateThreads > 1) {
            final ThreadFactory threadFactory = new NotificationProcessorThreadFactory(
                    new ThreadGroup("PeriodicUpdateGraph-updateExecutors"), "updateExecutor");
            return new ConcurrentNotificationProcessor(threadFactory, updateThreads);
        } else {
            return new QueueNotificationProcessor();
        }
    }

    @TestUseOnly
    private NotificationProcessor makeRandomizedNotificationProcessor(final Random random, final int nThreads,
            final int notificationStartDelay) {
        final ThreadFactory threadFactory = new NotificationProcessorThreadFactory(
                new ThreadGroup("PeriodicUpdateGraph-randomizedUpdatedExecutors"), "randomizedUpdateExecutor");
        return new ConcurrentNotificationProcessor(threadFactory, nThreads) {

            private Notification addRandomDelay(@NotNull final Notification notification) {
                if (notificationStartDelay <= 0) {
                    return notification;
                }
                return new NotificationAdapter(notification) {
                    @Override
                    public void run() {
                        final int millis = random.nextInt(notificationStartDelay);
                        logDependencies().append(Thread.currentThread().getName()).append(": Sleeping for  ")
                                .append(millis).append("ms").endl();
                        SleepUtil.sleep(millis);
                        super.run();
                    }
                };
            }

            @Override
            public void submit(@NotNull Notification notification) {
                if (notification instanceof UpdateSourceRefreshNotification) {
                    super.submit(notification);
                } else if (notification instanceof ErrorNotification) {
                    // NB: The previous implementation of this concept was more rigorous about ensuring that errors
                    // would be next, but this is likely good enough.
                    submitAt(notification, 0);
                } else {
                    submitAt(addRandomDelay(notification), random.nextInt(outstandingNotificationsCount() + 1));
                }
            }

            @Override
            public void submitAll(@NotNull IntrusiveDoublyLinkedQueue<Notification> notifications) {
                notifications.forEach(this::submit);
            }
        };
    }

    /**
     * Retrieve the number of update threads.
     *
     * <p>
     * The PeriodicUpdateGraph has a configurable number of update processing threads. The number of threads is exposed
     * in your method to enable you to partition a query based on the number of threads.
     * </p>
     *
     * @return the number of update threads configured.
     */
    @Override
    public int parallelismFactor() {
        if (notificationProcessor == null) {
            return updateThreads;
        } else if (notificationProcessor instanceof ConcurrentNotificationProcessor) {
            return ((ConcurrentNotificationProcessor) notificationProcessor).threadCount();
        } else {
            return 1;
        }
    }

    /**
     * Set the target duration of an update cycle, including the updating phase and the idle phase. This is also the
     * target interval between the start of one cycle and the start of the next.
     * <p>
     * Can be reset to default via {@link #resetTargetCycleDuration()}.
     *
     * @implNote Any target cycle duration {@code < 0} will be clamped to 0.
     *
     * @param targetCycleDurationMillis The target duration for update cycles in milliseconds
     */
    public void setTargetCycleDurationMillis(final long targetCycleDurationMillis) {
        this.targetCycleDurationMillis = Math.max(targetCycleDurationMillis, 0);
    }

    /**
     * Get the target duration of an update cycle, including the updating phase and the idle phase. This is also the
     * target interval between the start of one cycle and the start of the next.
     *
     * @return The {@link #setTargetCycleDurationMillis(long) current} target cycle duration
     */
    public long getTargetCycleDurationMillis() {
        return targetCycleDurationMillis;
    }

    @Override
    public boolean isCycleOnBudget(long cycleTimeNanos) {
        return cycleTimeNanos <= MILLISECONDS.toNanos(targetCycleDurationMillis);
    }

    /**
     * Resets the run cycle time to the default target configured via the {@link Builder} setting.
     *
     * @implNote If the {@link Builder#targetCycleDurationMillis(long)} property is not set, this value defaults to
     *           {@link Builder#DEFAULT_TARGET_CYCLE_DURATION_MILLIS_PROP} which defaults to 1000ms.
     */
    @SuppressWarnings("unused")
    public void resetTargetCycleDuration() {
        targetCycleDurationMillis = defaultTargetCycleDurationMillis;
    }

    /**
     * <p>
     * Enable unit test mode.
     * </p>
     *
     * <p>
     * In this mode calls to {@link #addSource(Runnable)} will only mark tables as
     * {@link DynamicNode#setRefreshing(boolean) refreshing}. Additionally {@link #start()} may not be called.
     * </p>
     */
    public void enableUnitTestMode() {
        if (unitTestMode) {
            return;
        }
        if (!allowUnitTestMode) {
            throw new IllegalStateException("PeriodicUpdateGraph.allowUnitTestMode=false");
        }
        if (refreshThread != null) {
            throw new IllegalStateException("PeriodicUpdateGraph.refreshThread is executing!");
        }
        resetLock();
        unitTestMode = true;
        unitTestRefreshThreadPool = makeUnitTestRefreshExecutor();
        updatePerformanceTracker.enableUnitTestMode();
    }

    /**
     * @return whether unit test mode is allowed
     */
    public boolean isUnitTestModeAllowed() {
        return allowUnitTestMode;
    }

    /**
     * Enable the loop watchdog with the specified timeout. A value of 0 disables the watchdog.
     *
     * @implNote Any timeout less than 0 will be clamped to 0.
     *
     * @param watchDogMillis The time in milliseconds to set the watchdog, or 0 to disable.
     */
    public void setWatchDogMillis(int watchDogMillis) {
        this.watchDogMillis = Math.max(watchDogMillis, 0);
    }

    /**
     * Get the current watchdog {@link #setWatchDogMillis(int) timeout} value.
     *
     * @return The current timeout for the watchdog, 0 for disabled
     */
    public int getWatchDogMillis() {
        return watchDogMillis;
    }

    /**
     * Set the procedure to be called when the watchdog {@link #setWatchDogMillis(int) times out}.
     *
     * @param procedure The procedure to call
     */
    public void setWatchDogTimeoutProcedure(LongConsumer procedure) {
        this.watchDogTimeoutProcedure = procedure;
    }

    /**
     * Install a real NotificationProcessor and start the primary refresh thread.
     *
     * @implNote Must not be in {@link #enableUnitTestMode() unit test} mode.
     */
    public void start() {
        Assert.eqTrue(running, "running");
        Assert.eqFalse(unitTestMode, "unitTestMode");
        Assert.eqFalse(allowUnitTestMode, "allowUnitTestMode");
        synchronized (this) {
            if (watchdogScheduler == null) {
                watchdogScheduler = Executors.newSingleThreadScheduledExecutor(
                        new NamingThreadFactory(PeriodicUpdateGraph.class, "watchdogScheduler", true) {
                            @Override
                            public Thread newThread(@NotNull final Runnable r) {
                                // Not a refresh thread, but should still be instrumented for debugging purposes.
                                return super.newThread(threadInitializationFactory.createInitializer(r));
                            }
                        });
            }
            if (notificationProcessor instanceof PoisonedNotificationProcessor) {
                notificationProcessor = makeNotificationProcessor();
            }
            if (refreshThread == null) {
                final OperationInitializer operationInitializer =
                        ExecutionContext.getContext().getOperationInitializer();
                refreshThread = new Thread(threadInitializationFactory.createInitializer(() -> {
                    configureRefreshThread(operationInitializer);
                    while (running) {
                        Assert.eqFalse(this.allowUnitTestMode, "allowUnitTestMode");
                        refreshTablesAndFlushNotifications();
                    }
                }), "PeriodicUpdateGraph." + getName() + ".refreshThread");
                refreshThread.setDaemon(true);
                log.info().append("PeriodicUpdateGraph starting with ").append(updateThreads)
                        .append(" notification processing threads").endl();
                updatePerformanceTracker.start();
                refreshThread.start();
            }
        }
    }

    /**
     * Begins the process to stop all processing threads and forces ReferenceCounted sources to a reference count of
     * zero.
     */
    @Override
    public void stop() {
        running = false;
        notificationProcessor.shutdown();
        // ensure that any outstanding cycle has completed
        exclusiveLock().doLocked(() -> {
        });
    }

    /**
     * {@inheritDoc}
     *
     * @implNote This will do nothing in {@link #enableUnitTestMode() unit test} mode other than mark the table as
     *           refreshing.
     */
    @Override
    public void addSource(@NotNull Runnable updateSource) {
        if (allowUnitTestMode) {
            // if we are in unit test mode we never want to start the UpdateGraph
            if (updateSource instanceof DynamicNode) {
                ((DynamicNode) updateSource).setRefreshing(true);
            }
            return;
        }
        super.addSource(updateSource);
        start();
    }

    /**
     * Enqueue a notification to be flushed according to its priority. Non-terminal notifications should only be
     * enqueued during the updating phase of a cycle. That is, they should be enqueued from an update source or
     * subsequent notification delivery.
     *
     * @param notification The notification to enqueue
     * @see NotificationQueue.Notification#isTerminal()
     * @see LogicalClock.State
     */
    @Override
    public void addNotification(@NotNull final Notification notification) {
        if (notificationAdditionDelay > 0) {
            SleepUtil.sleep(notificationRandomizer.nextInt(notificationAdditionDelay));
        }
        super.addNotification(notification);
    }

    @Override
    public boolean maybeAddNotification(@NotNull final Notification notification, final long deliveryStep) {
        if (notificationAdditionDelay > 0) {
            SleepUtil.sleep(notificationRandomizer.nextInt(notificationAdditionDelay));
        }
        return super.maybeAddNotification(notification, deliveryStep);
    }

    /**
     * Request that the next update cycle begin as soon as practicable. This "hurry-up" cycle happens through normal
     * means using the refresh thread and its workers.
     */
    @Override
    public void requestRefresh() {
        if (!running) {
            throw new IllegalStateException("Cannot request refresh when UpdateGraph is no longer running.");
        }
        refreshRequested.set(true);
        synchronized (refreshRequested) {
            refreshRequested.notify();
        }
    }

    /**
     * Clear all monitored tables and enqueued notifications to support {@link #enableUnitTestMode() unit-tests}.
     *
     * @param after Whether this is *after* a unit test completed. If true, held locks should result in an exception and
     *        the LivenessScopeStack will be cleared.
     */
    @TestUseOnly
    public void resetForUnitTests(final boolean after) {
        resetForUnitTests(after, false, 0, 0, 0, 0);
    }

    /**
     * Clear all monitored tables and enqueued notifications to support {@link #enableUnitTestMode() unit-tests}.
     *
     * @param after Whether this is *after* a unit test completed. If true, held locks should result in an exception and
     *        the LivenessScopeStack will be cleared.
     * @param randomizedNotifications Whether the notification processor should randomize the order of delivery
     * @param seed Seed for randomized notification delivery order and delays
     * @param maxRandomizedThreadCount Maximum number of threads handling randomized notification delivery
     * @param notificationStartDelay Maximum randomized notification start delay
     * @param notificationAdditionDelay Maximum randomized notification addition delay
     */
    @TestUseOnly
    public void resetForUnitTests(boolean after,
            final boolean randomizedNotifications, final int seed, final int maxRandomizedThreadCount,
            final int notificationStartDelay, final int notificationAdditionDelay) {
        final List<String> errors = new ArrayList<>();
        this.notificationRandomizer = new Random(seed);
        this.notificationAdditionDelay = notificationAdditionDelay;
        Assert.assertion(unitTestMode, "unitTestMode");

        resetForUnitTests(after, errors);

        if (randomizedNotifications) {
            notificationProcessor = makeRandomizedNotificationProcessor(notificationRandomizer,
                    maxRandomizedThreadCount, notificationStartDelay);
        } else {
            notificationProcessor = makeNotificationProcessor();
        }

        if (refreshThread != null) {
            errors.add("UpdateGraph refreshThread isAlive");
        }

        try {
            unitTestRefreshThreadPool.submit(() -> ensureUnlocked("unit test run pool thread", errors)).get();
        } catch (InterruptedException | ExecutionException e) {
            errors.add("Failed to ensure UpdateGraph unlocked from unit test run thread pool: " + e);
        }
        unitTestRefreshThreadPool.shutdownNow();
        try {
            if (!unitTestRefreshThreadPool.awaitTermination(1, TimeUnit.SECONDS)) {
                errors.add("Failed to cleanup jobs in unit test run thread pool");
            }
        } catch (InterruptedException e) {
            errors.add("Interrupted while trying to cleanup jobs in unit test run thread pool");
        }
        unitTestRefreshThreadPool = makeUnitTestRefreshExecutor();

        if (!errors.isEmpty()) {
            final String message =
                    "UpdateGraph reset for unit tests reported errors:\n\t" + String.join("\n\t", errors);
            System.err.println(message);
            if (after) {
                throw new IllegalStateException(message);
            }
        }

        resetLock();
    }

    /**
     * Begin the next {@link LogicalClockImpl#startUpdateCycle() update cycle} while in {@link #enableUnitTestMode()
     * unit-test} mode. Note that this happens on a simulated UpdateGraph run thread, rather than this thread. This
     * overload is the same as {@code startCycleForUnitTests(true)}.
     */
    @TestUseOnly
    public void startCycleForUnitTests() {
        startCycleForUnitTests(true);
    }

    /**
     * Begin the next {@link LogicalClockImpl#startUpdateCycle() update cycle} while in {@link #enableUnitTestMode()
     * unit-test} mode. Note that this happens on a simulated UpdateGraph run thread, rather than this thread.
     *
     * @param sourcesSatisfied Whether sources should be marked as satisfied by this invocation; if {@code false}, the
     *        caller must control source satisfaction using {@link #markSourcesRefreshedForUnitTests()}.
     */
    @TestUseOnly
    public void startCycleForUnitTests(final boolean sourcesSatisfied) {
        Assert.assertion(unitTestMode, "unitTestMode");
        try {
            unitTestRefreshThreadPool.submit(() -> startCycleForUnitTestsInternal(sourcesSatisfied)).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new UncheckedDeephavenException(e);
        }
    }

    @TestUseOnly
    private void startCycleForUnitTestsInternal(final boolean sourcesSatisfied) {
        // noinspection AutoBoxing
        isUpdateThread.set(true);
        exclusiveLock().lock();

        Assert.eqNull(refreshScope, "refreshScope");
        refreshScope = new LivenessScope();
        LivenessScopeStack.push(refreshScope);

        logicalClock.startUpdateCycle();
        if (sourcesSatisfied) {
            markSourcesRefreshedForUnitTests();
        }
    }

    /**
     * Record that sources have been satisfied within a unit test cycle.
     */
    @TestUseOnly
    public void markSourcesRefreshedForUnitTests() {
        Assert.assertion(unitTestMode, "unitTestMode");
        updateSourcesLastSatisfiedStep(true);
    }

    /**
     * Do the second half of the update cycle, including flushing notifications, and completing the
     * {@link LogicalClockImpl#completeUpdateCycle() LogicalClock} update cycle. Note that this happens on a simulated
     * UpdateGraph run thread, rather than this thread.
     */
    @TestUseOnly
    public void completeCycleForUnitTests() {
        completeCycleForUnitTests(false);
    }

    /**
     * Do the second half of the update cycle, including flushing notifications, and completing the
     * {@link LogicalClockImpl#completeUpdateCycle() LogicalClock} update cycle. Note that this happens on a simulated
     * UpdateGraph run thread, rather than this thread.
     *
     * @param errorCaughtAndInFinallyBlock Whether an error was caught, and we are in a {@code finally} block
     */
    private void completeCycleForUnitTests(boolean errorCaughtAndInFinallyBlock) {
        Assert.assertion(unitTestMode, "unitTestMode");
        if (!errorCaughtAndInFinallyBlock) {
            final long currentStep = logicalClock.currentStep();
            final boolean satisfied = satisfied(currentStep);
            Assert.assertion(satisfied, "satisfied()", currentStep, "currentStep");
        }
        try {
            unitTestRefreshThreadPool.submit(this::completeCycleForUnitTestsInternal).get();
        } catch (InterruptedException | ExecutionException e) {
            if (!errorCaughtAndInFinallyBlock) {
                throw new UncheckedDeephavenException(e);
            }
        }
    }

    @TestUseOnly
    private void completeCycleForUnitTestsInternal() {
        try (final SafeCloseable ignored = () -> {
            if (refreshScope != null) {
                LivenessScopeStack.pop(refreshScope);
                refreshScope.release();
                refreshScope = null;
            }

            exclusiveLock().unlock();
            isUpdateThread.remove();
        }) {
            flushNotificationsAndCompleteCycle(false);
        }
    }

    /**
     * Execute the given runnable wrapped with {@link #startCycleForUnitTests()} and
     * {@link #completeCycleForUnitTests()}. Note that the runnable is run on the current thread. This is equivalent to
     * {@code runWithinUnitTestCycle(runnable, true)}.
     *
     * @param runnable The runnable to execute
     */
    @TestUseOnly
    public <T extends Exception> void runWithinUnitTestCycle(@NotNull final ThrowingRunnable<T> runnable) throws T {
        runWithinUnitTestCycle(runnable, true);
    }

    /**
     * Execute the given runnable wrapped with {@link #startCycleForUnitTests()} and
     * {@link #completeCycleForUnitTests()}. Note that the runnable is run on the current thread.
     *
     * @param runnable The runnable to execute
     * @param sourcesSatisfied Whether sources should be marked as satisfied by this invocation; if {@code false}, the
     *        caller must control source satisfaction using {@link #markSourcesRefreshedForUnitTests()}.
     */
    @TestUseOnly
    public <T extends Exception> void runWithinUnitTestCycle(
            @NotNull final ThrowingRunnable<T> runnable,
            final boolean sourcesSatisfied)
            throws T {
        startCycleForUnitTests(sourcesSatisfied);
        boolean errorCaught = false;
        try {
            runnable.run();
        } catch (final Throwable err) {
            errorCaught = true;
            throw err;
        } finally {
            completeCycleForUnitTests(errorCaught);
        }
    }

    /**
     * Refresh an update source on a simulated UpdateGraph run thread, rather than this thread.
     *
     * @param updateSource The update source to run
     */
    @TestUseOnly
    public void refreshUpdateSourceForUnitTests(@NotNull final Runnable updateSource) {
        Assert.assertion(unitTestMode, "unitTestMode");
        try {
            unitTestRefreshThreadPool.submit(updateSource).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new UncheckedDeephavenException(e);
        }
    }

    /**
     * Flush a single notification from the UpdateGraph queue. Note that this happens on a simulated UpdateGraph run
     * thread, rather than this thread.
     *
     * @return whether a notification was found in the queue
     */
    @TestUseOnly
    public boolean flushOneNotificationForUnitTests() {
        return flushOneNotificationForUnitTests(false);
    }

    /**
     * Flush a single notification from the UpdateGraph queue. Note that this happens on a simulated UpdateGraph run
     * thread, rather than this thread.
     *
     * @param expectOnlyUnsatisfiedNotifications Whether we expect there to be only unsatisfied notifications pending
     * @return whether a notification was found in the queue
     */
    @TestUseOnly
    public boolean flushOneNotificationForUnitTests(final boolean expectOnlyUnsatisfiedNotifications) {
        Assert.assertion(unitTestMode, "unitTestMode");

        final NotificationProcessor existingNotificationProcessor = notificationProcessor;
        try {
            this.notificationProcessor = new ControlledNotificationProcessor();
            // noinspection AutoUnboxing,AutoBoxing
            return unitTestRefreshThreadPool.submit(
                    () -> flushOneNotificationForUnitTestsInternal(expectOnlyUnsatisfiedNotifications)).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new UncheckedDeephavenException(e);
        } finally {
            this.notificationProcessor = existingNotificationProcessor;
        }
    }

    @TestUseOnly
    private boolean flushOneNotificationForUnitTestsInternal(final boolean expectOnlyUnsatisfiedNotifications) {
        final IntrusiveDoublyLinkedQueue<Notification> pendingToEvaluate =
                new IntrusiveDoublyLinkedQueue<>(IntrusiveDoublyLinkedNode.Adapter.<Notification>getInstance());
        notificationProcessor.beforeNotificationsDrained();
        synchronized (pendingNormalNotifications) {
            pendingToEvaluate.transferAfterTailFrom(pendingNormalNotifications);
        }
        final boolean somethingWasPending = !pendingToEvaluate.isEmpty();
        Notification satisfied = null;
        for (final Iterator<Notification> it = pendingToEvaluate.iterator(); it.hasNext();) {
            final Notification notification = it.next();

            Assert.eqFalse(notification.isTerminal(), "notification.isTerminal()");
            Assert.eqFalse(notification.mustExecuteWithUpdateGraphLock(),
                    "notification.mustExecuteWithUpdateGraphLock()");

            if (notification.canExecute(logicalClock.currentStep())) {
                satisfied = notification;
                it.remove();
                break;
            }
        }
        synchronized (pendingNormalNotifications) {
            pendingNormalNotifications.transferBeforeHeadFrom(pendingToEvaluate);
        }
        if (satisfied != null) {
            notificationProcessor.submit(satisfied);
            if (expectOnlyUnsatisfiedNotifications) {
                // noinspection ThrowableNotThrown
                Assert.statementNeverExecuted(
                        "Flushed a notification in unit test mode, but expected only unsatisfied pending notifications");
            }
        } else if (somethingWasPending && !expectOnlyUnsatisfiedNotifications) {
            // noinspection ThrowableNotThrown
            Assert.statementNeverExecuted(
                    "Did not flush any notifications in unit test mode, yet there were outstanding notifications");
        }
        return satisfied != null;
    }

    /**
     * Flush all the normal notifications from the UpdateGraph queue. Note that the flushing happens on a simulated
     * UpdateGraph run thread, rather than this thread.
     */
    @TestUseOnly
    public void flushAllNormalNotificationsForUnitTests() {
        flushAllNormalNotificationsForUnitTests(() -> true, 0).run();
    }

    /**
     * Flush all the normal notifications from the UpdateGraph queue, continuing until {@code done} returns
     * {@code true}. Note that the flushing happens on a simulated UpdateGraph run thread, rather than this thread.
     *
     * @param done Function to determine when we can stop waiting for new notifications
     * @return A Runnable that may be used to wait for the concurrent flush job to complete
     */
    @TestUseOnly
    public Runnable flushAllNormalNotificationsForUnitTests(@NotNull final BooleanSupplier done,
            final long timeoutMillis) {
        Assert.assertion(unitTestMode, "unitTestMode");
        Assert.geqZero(timeoutMillis, "timeoutMillis");

        final NotificationProcessor existingNotificationProcessor = notificationProcessor;
        final ControlledNotificationProcessor controlledNotificationProcessor = new ControlledNotificationProcessor();
        notificationProcessor = controlledNotificationProcessor;
        final Future<?> flushJobFuture = unitTestRefreshThreadPool.submit(() -> {
            final long deadlineNanoTime = System.nanoTime() + MILLISECONDS.toNanos(timeoutMillis);
            boolean flushed;
            while ((flushed = flushOneNotificationForUnitTestsInternal(false)) || !done.getAsBoolean()) {
                if (!flushed) {
                    final long remainingNanos = deadlineNanoTime - System.nanoTime();
                    if (!controlledNotificationProcessor.blockUntilNotificationAdded(remainingNanos)) {
                        // noinspection ThrowableNotThrown
                        Assert.statementNeverExecuted(
                                "Unit test failure due to timeout after " + timeoutMillis + " ms");
                    }
                }
            }
        });
        return () -> {
            try {
                flushJobFuture.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new UncheckedDeephavenException(e);
            } finally {
                notificationProcessor = existingNotificationProcessor;
            }
        };
    }

    /**
     * If the run thread is waiting in flushNormalNotificationsAndCompleteCycle() or
     * {@link #flushAllNormalNotificationsForUnitTests(BooleanSupplier, long)}, wake it up.
     */
    @TestUseOnly
    public void wakeRefreshThreadForUnitTests() {
        // Pretend we may have added a notification
        notificationProcessor.onNotificationAdded();
    }


    private class ConcurrentNotificationProcessor implements NotificationProcessor {

        private final IntrusiveDoublyLinkedQueue<Notification> satisfiedNotifications =
                new IntrusiveDoublyLinkedQueue<>(IntrusiveDoublyLinkedNode.Adapter.<Notification>getInstance());
        private final Thread[] updateThreads;

        private final AtomicInteger outstandingNotifications = new AtomicInteger(0);
        private final Semaphore pendingNormalNotificationsCheckNeeded = new Semaphore(0, false);

        private volatile boolean running = true;
        private volatile boolean isHealthy = true;

        public ConcurrentNotificationProcessor(@NotNull final ThreadFactory threadFactory,
                final int updateThreadCount) {
            updateThreads = new Thread[updateThreadCount];
            for (int ti = 0; ti < updateThreadCount; ++ti) {
                updateThreads[ti] = threadFactory.newThread(this::processSatisfiedNotifications);
                updateThreads[ti].start();
            }
        }

        private void processSatisfiedNotifications() {
            log.info().append(Thread.currentThread().getName())
                    .append(": starting to poll for satisfied notifications");
            Notification satisfiedNotification = null;
            try {
                while (running) {
                    synchronized (satisfiedNotifications) {
                        while (running && (satisfiedNotification = satisfiedNotifications.poll()) == null) {
                            try {
                                satisfiedNotifications.wait();
                            } catch (InterruptedException ignored) {
                            }
                        }
                    }
                    if (satisfiedNotification == null) {
                        break;
                    }

                    runNotification(satisfiedNotification);
                    satisfiedNotification = null;
                    outstandingNotifications.decrementAndGet();
                    pendingNormalNotificationsCheckNeeded.release();
                }
            } finally {
                if (satisfiedNotification != null) {
                    // if we were thrown out of the loop; decrement / release after setting the unhealthy flag
                    isHealthy = false;
                    outstandingNotifications.decrementAndGet();
                    pendingNormalNotificationsCheckNeeded.release();
                }
                log.info().append(Thread.currentThread().getName()).append(": terminating");
            }
        }

        @Override
        public void submit(@NotNull final Notification notification) {
            outstandingNotifications.incrementAndGet();
            synchronized (satisfiedNotifications) {
                satisfiedNotifications.offer(notification);
                satisfiedNotifications.notify();
            }
        }

        @Override
        public void submitAll(@NotNull IntrusiveDoublyLinkedQueue<Notification> notifications) {
            outstandingNotifications.addAndGet(notifications.size());
            synchronized (satisfiedNotifications) {
                satisfiedNotifications.transferAfterTailFrom(notifications);
                satisfiedNotifications.notifyAll();
            }
        }

        @TestUseOnly
        protected void submitAt(@NotNull final Notification notification, final int offset) {
            outstandingNotifications.incrementAndGet();
            synchronized (satisfiedNotifications) {
                // We clamp the size here because there's a race between the random offset selection and other threads
                // draining the queue of satisfied notifications.
                satisfiedNotifications.insert(notification, Math.min(offset, satisfiedNotifications.size()));
                satisfiedNotifications.notify();
            }
        }

        @Override
        public int outstandingNotificationsCount() {
            return outstandingNotifications.get();
        }

        @Override
        public void doWork() {
            try {
                pendingNormalNotificationsCheckNeeded.acquire();
                // if a processing thread exits unexpectedly, propagate an error to the outer refresh thread
                Assert.eqTrue(isHealthy, "isHealthy");
            } catch (InterruptedException ignored) {
            }
        }

        @Override
        public void doAllWork() {
            while (outstandingNotificationsCount() > 0) {
                doWork();
            }
            // A successful and a failed notification may race the release of pendingNormalNotificationsCheckNeeded,
            // causing this thread to miss a false isHealthy. Since isHealthy is set prior to decrementing
            // outstandingNotificationsCount, we're guaranteed to read the correct value after exiting the while loop.
            Assert.eqTrue(isHealthy, "isHealthy");
        }

        @Override
        public void shutdown() {
            running = false;
            synchronized (satisfiedNotifications) {
                satisfiedNotifications.clear();
                satisfiedNotifications.notifyAll();
            }
            for (final Thread updateThread : updateThreads) {
                try {
                    updateThread.join();
                } catch (InterruptedException ignored) {
                }
            }
        }

        @Override
        public void onNotificationAdded() {
            pendingNormalNotificationsCheckNeeded.release();
        }

        @Override
        public void beforeNotificationsDrained() {
            pendingNormalNotificationsCheckNeeded.drainPermits();
        }

        int threadCount() {
            return updateThreads.length;
        }
    }

    @TestUseOnly
    private class ControlledNotificationProcessor implements NotificationProcessor {

        private final Semaphore pendingNormalNotificationsCheckNeeded = new Semaphore(0, false);

        @Override
        public void submit(@NotNull final Notification notification) {
            runNotification(notification);
        }

        @Override
        public void submitAll(@NotNull final IntrusiveDoublyLinkedQueue<Notification> notifications) {
            Notification notification;
            while ((notification = notifications.poll()) != null) {
                runNotification(notification);
            }
        }

        @Override
        public int outstandingNotificationsCount() {
            return 0;
        }

        @Override
        public void doWork() {
            // noinspection ThrowableNotThrown
            Assert.statementNeverExecuted();
        }

        @Override
        public void doAllWork() {
            // noinspection ThrowableNotThrown
            Assert.statementNeverExecuted();
        }

        @Override
        public void shutdown() {
            // noinspection ThrowableNotThrown
            Assert.statementNeverExecuted();
        }

        @Override
        public void onNotificationAdded() {
            pendingNormalNotificationsCheckNeeded.release();
        }

        @Override
        public void beforeNotificationsDrained() {
            pendingNormalNotificationsCheckNeeded.drainPermits();
        }

        private boolean blockUntilNotificationAdded(final long nanosToWait) {
            try {
                return pendingNormalNotificationsCheckNeeded.tryAcquire(nanosToWait, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                // noinspection ThrowableNotThrown
                Assert.statementNeverExecuted();
                return false;
            }
        }
    }


    /**
     * Iterate over all monitored tables and run them.
     *
     * <p>
     * This method also ensures that the loop runs no faster than {@link #getTargetCycleDurationMillis() minimum cycle
     * time}.
     * </p>
     */
    @Override
    void refreshTablesAndFlushNotifications() {
        final long startTimeNanos = System.nanoTime();

        ScheduledFuture<?> watchdogFuture = null;
        final long localWatchdogMillis = watchDogMillis;
        final LongConsumer localWatchdogTimeoutProcedure = watchDogTimeoutProcedure;
        if ((localWatchdogMillis > 0) && (localWatchdogTimeoutProcedure != null)) {
            watchdogFuture = watchdogScheduler.schedule(
                    () -> localWatchdogTimeoutProcedure.accept(localWatchdogMillis),
                    localWatchdogMillis, MILLISECONDS);
        }

        super.refreshTablesAndFlushNotifications();

        if (watchdogFuture != null) {
            watchdogFuture.cancel(true);
        }

        if (interCycleYield) {
            Thread.yield();
        }

        waitForNextCycle(startTimeNanos);
    }

    /**
     * <p>
     * Ensure that at least {@link #getTargetCycleDurationMillis() minCycleTime} has passed before returning.
     * </p>
     *
     * <p>
     * If the delay is interrupted by a {@link UpdateSourceRegistrar#requestRefresh() request} to run a single table
     * this task will drain the queue of single run requests, then continue to wait for a complete period if necessary.
     * </p>
     *
     * <p>
     * If the delay is interrupted for any other {@link InterruptedException reason}, it will be logged and continue to
     * wait the remaining period.
     * </p>
     *
     * @param startTimeNanos The start time of the last run cycle as reported by {@link System#nanoTime()}
     */
    private void waitForNextCycle(final long startTimeNanos) {
        final long nowNanos = System.nanoTime();
        long expectedEndTimeNanos = startTimeNanos + MILLISECONDS.toNanos(targetCycleDurationMillis);
        if (minimumInterCycleSleep > 0) {
            expectedEndTimeNanos =
                    Math.max(expectedEndTimeNanos, nowNanos + MILLISECONDS.toNanos(minimumInterCycleSleep));
        }
        maybeFlushUpdatePerformance(nowNanos, expectedEndTimeNanos);
        waitForEndTime(expectedEndTimeNanos);
    }

    /**
     * <p>
     * Ensure the current time is past {@code expectedEndTime} before returning, or return early if an immediate refresh
     * is requested.
     * <p>
     * If the delay is interrupted for any other {@link InterruptedException reason}, it will be logged and continue to
     * wait the remaining period.
     *
     * @param expectedEndTimeNanos The time (as reported by {@link System#nanoTime()}) which we should sleep until
     */
    private void waitForEndTime(final long expectedEndTimeNanos) {
        long remainingNanos;
        while ((remainingNanos = expectedEndTimeNanos - System.nanoTime()) > 0) {
            if (refreshRequested.get()) {
                return;
            }
            synchronized (refreshRequested) {
                if (refreshRequested.get()) {
                    return;
                }
                final long millisToWait = remainingNanos / 1_000_000;
                final int extraNanosToWait = (int) (remainingNanos - (millisToWait * 1_000_000));
                try {
                    refreshRequested.wait(millisToWait, extraNanosToWait);
                } catch (final InterruptedException logAndIgnore) {
                    log.warn().append("Interrupted while waiting on refreshRequested. Ignoring: ").append(logAndIgnore)
                            .endl();
                }
            }
        }
    }

    @Override
    void refreshAllTables() {
        refreshRequested.set(false);
        super.refreshAllTables();
    }

    private class NotificationProcessorThreadFactory extends NamingThreadFactory {
        private NotificationProcessorThreadFactory(@NotNull final ThreadGroup threadGroup, @NotNull final String name) {
            super(threadGroup, PeriodicUpdateGraph.class, name, true);
        }

        @Override
        public Thread newThread(@NotNull final Runnable r) {
            OperationInitializer captured = ExecutionContext.getContext().getOperationInitializer();
            return super.newThread(threadInitializationFactory.createInitializer(() -> {
                configureRefreshThread(captured);
                r.run();
            }));
        }
    }

    private ExecutorService makeUnitTestRefreshExecutor() {
        return Executors.newFixedThreadPool(1, new UnitTestThreadFactory());
    }

    @TestUseOnly
    private class UnitTestThreadFactory extends NamingThreadFactory {

        private UnitTestThreadFactory() {
            super(PeriodicUpdateGraph.class, "unitTestRefresh");
        }

        @Override
        public Thread newThread(@NotNull final Runnable r) {
            OperationInitializer captured = ExecutionContext.getContext().getOperationInitializer();
            return super.newThread(() -> {
                configureUnitTestRefreshThread(captured);
                r.run();
            });
        }
    }

    /**
     * Configure the primary UpdateGraph thread or one of the auxiliary notification processing threads.
     */
    private void configureRefreshThread(OperationInitializer captured) {
        SystemicObjectTracker.markThreadSystemic();
        MultiChunkPool.enableDedicatedPoolForThisThread();
        isUpdateThread.set(true);
        // Install this UpdateGraph via ExecutionContext for refresh threads, share the same operation initializer
        // noinspection resource
        ExecutionContext.newBuilder().setUpdateGraph(this).setOperationInitializer(captured).build().open();
    }

    /**
     * Configure threads to be used for unit test processing.
     */
    private void configureUnitTestRefreshThread(OperationInitializer captured) {
        final Thread currentThread = Thread.currentThread();
        final Thread.UncaughtExceptionHandler existing = currentThread.getUncaughtExceptionHandler();
        currentThread.setUncaughtExceptionHandler((final Thread errorThread, final Throwable throwable) -> {
            ensureUnlocked("unit test run pool thread exception handler", null);
            existing.uncaughtException(errorThread, throwable);
        });
        isUpdateThread.set(true);
        // Install this UpdateGraph and share operation initializer pool via ExecutionContext for refresh threads
        // noinspection resource
        ExecutionContext.newBuilder().setUpdateGraph(this).setOperationInitializer(captured).build().open();
    }

    public static PeriodicUpdateGraph getInstance(final String name) {
        return BaseUpdateGraph.getInstance(name).cast();
    }

    public static final class Builder {
        private final boolean allowUnitTestMode =
                Configuration.getInstance().getBooleanWithDefault(ALLOW_UNIT_TEST_MODE_PROP, false);
        private long targetCycleDurationMillis =
                Configuration.getInstance().getIntegerWithDefault(DEFAULT_TARGET_CYCLE_DURATION_MILLIS_PROP, 1000);
        private long minimumCycleDurationToLogNanos = DEFAULT_MINIMUM_CYCLE_DURATION_TO_LOG_NANOSECONDS;

        private String name;
        private int numUpdateThreads = -1;
        private ThreadInitializationFactory threadInitializationFactory = runnable -> runnable;

        public Builder(String name) {
            this.name = name;
        }

        /**
         * Set the target duration of an update cycle, including the updating phase and the idle phase. This is also the
         * target interval between the start of one cycle and the start of the next.
         *
         * @implNote Any target cycle duration {@code < 0} will be clamped to 0.
         *
         * @param targetCycleDurationMillis The target duration for update cycles in milliseconds
         * @return this builder
         */
        public Builder targetCycleDurationMillis(long targetCycleDurationMillis) {
            this.targetCycleDurationMillis = targetCycleDurationMillis;
            return this;
        }

        /**
         * Set the minimum duration of an update cycle that should be logged at the INFO level.
         *
         * @param minimumCycleDurationToLogNanos threshold to log a slow cycle
         * @return this builder
         */
        public Builder minimumCycleDurationToLogNanos(long minimumCycleDurationToLogNanos) {
            this.minimumCycleDurationToLogNanos = minimumCycleDurationToLogNanos;
            return this;
        }

        /**
         * Sets the number of threads to use in the update graph processor. Values &lt; 0 indicate to use one thread per
         * available processor.
         *
         * @param numUpdateThreads number of threads to use in update processing
         * @return this builder
         */
        public Builder numUpdateThreads(int numUpdateThreads) {
            this.numUpdateThreads = numUpdateThreads;
            return this;
        }

        /**
         * Sets a functional interface that adds custom initialization for threads started by this UpdateGraph.
         *
         * @param threadInitializationFactory the function to invoke on any runnables that will be used to start threads
         * @return this builder
         */
        public Builder threadInitializationFactory(ThreadInitializationFactory threadInitializationFactory) {
            this.threadInitializationFactory = threadInitializationFactory;
            return this;
        }

        /**
         * Constructs and returns a PeriodicUpdateGraph. It is an error to do so an instance already exists with the
         * name provided to this builder.
         *
         * @return the new PeriodicUpdateGraph
         * @throws IllegalStateException if an UpdateGraph with the provided name already exists
         */
        public PeriodicUpdateGraph build() {
            return BaseUpdateGraph.buildOrThrow(name, this::construct);
        }

        /**
         * Returns an existing PeriodicUpdateGraph with the name provided to this Builder, if one exists, else returns a
         * new PeriodicUpdateGraph.
         *
         * @return the PeriodicUpdateGraph
         * @throws ClassCastException if the existing graph is not a PeriodicUpdateGraph
         */
        public PeriodicUpdateGraph existingOrBuild() {
            return BaseUpdateGraph.existingOrBuild(name, this::construct).cast();
        }

        private PeriodicUpdateGraph construct() {
            return new PeriodicUpdateGraph(
                    name,
                    allowUnitTestMode,
                    targetCycleDurationMillis,
                    minimumCycleDurationToLogNanos,
                    numUpdateThreads,
                    threadInitializationFactory);
        }
    }
}
