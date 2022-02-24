/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.updategraph;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.SleepUtil;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.reference.SimpleReference;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.chunk.util.pools.MultiChunkPool;
import io.deephaven.engine.liveness.LivenessManager;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.util.reference.CleanupReferenceProcessorInstance;
import io.deephaven.engine.util.systemicmarking.SystemicObjectTracker;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.log.LogEntry;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.io.logger.Logger;
import io.deephaven.io.sched.Scheduler;
import io.deephaven.io.sched.TimedJob;
import io.deephaven.net.CommBase;
import io.deephaven.util.FunctionalInterfaces;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.TestUseOnly;
import io.deephaven.util.datastructures.SimpleReferenceManager;
import io.deephaven.util.datastructures.linked.IntrusiveDoublyLinkedNode;
import io.deephaven.util.datastructures.linked.IntrusiveDoublyLinkedQueue;
import io.deephaven.util.locks.AwareFunctionalLock;
import io.deephaven.util.process.ProcessEnvironment;
import io.deephaven.util.thread.NamingThreadFactory;
import io.deephaven.util.thread.ThreadDump;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.WeakReference;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

/**
 * <p>
 * This class uses a thread (or pool of threads) to periodically update a set of monitored update sources at a specified
 * target cycle interval. The target cycle interval can be {@link #setTargetCycleDurationMillis(long) configured} to
 * reduce or increase the run rate of the monitored sources.
 * <p>
 * This class can be configured via the following {@link Configuration} property
 * <ul>
 * <li>{@value DEFAULT_TARGET_CYCLE_DURATION_MILLIS_PROP}(optional)</i> - The default target cycle time in ms (1000 if
 * not defined)</li>
 * </ul>
 */
public enum UpdateGraphProcessor implements UpdateSourceRegistrar, NotificationQueue, NotificationQueue.Dependency {
    DEFAULT;

    private final Logger log = LoggerFactory.getLogger(UpdateGraphProcessor.class);

    /**
     * Update sources that are part of this UpdateGraphProcessor.
     */
    private final SimpleReferenceManager<Runnable, UpdateSourceRefreshNotification> sources =
            new SimpleReferenceManager<>(UpdateSourceRefreshNotification::new);

    /**
     * Recorder for updates source satisfaction as a phase of notification processing.
     */
    private volatile long sourcesLastSatisfiedStep;

    /**
     * The queue of non-terminal notifications to process.
     */
    private final IntrusiveDoublyLinkedQueue<Notification> pendingNormalNotifications =
            new IntrusiveDoublyLinkedQueue<>(IntrusiveDoublyLinkedNode.Adapter.<Notification>getInstance());

    /**
     * The queue of terminal notifications to process.
     */
    private final IntrusiveDoublyLinkedQueue<Notification> terminalNotifications =
            new IntrusiveDoublyLinkedQueue<>(IntrusiveDoublyLinkedNode.Adapter.<Notification>getInstance());

    /**
     * A flag indicating that an accelerated cycle has been requested.
     */
    private final AtomicBoolean refreshRequested = new AtomicBoolean();

    private final Thread refreshThread;

    /**
     * If this is set to a positive value, then we will call the {@link #watchDogTimeoutProcedure} if any single run
     * loop takes longer than this value. The intention is to use this for strategies, or other queries, where a
     * UpdateGraphProcessor loop that is "stuck" is the equivalent of an error. Set the value with
     * {@link #setWatchDogMillis(int)}.
     */
    private int watchDogMillis = 0;
    /**
     * If a timeout time has been {@link #setWatchDogMillis(int) set}, this procedure will be called if any single run
     * loop takes longer than the value specified. Set the value with
     * {@link #setWatchDogTimeoutProcedure(LongConsumer)}.
     */
    private LongConsumer watchDogTimeoutProcedure = null;

    public static final String ALLOW_UNIT_TEST_MODE_PROP = "UpdateGraphProcessor.allowUnitTestMode";
    private final boolean allowUnitTestMode =
            Configuration.getInstance().getBooleanWithDefault(ALLOW_UNIT_TEST_MODE_PROP, false);
    private int notificationAdditionDelay = 0;
    private Random notificationRandomizer = new Random(0);
    private boolean unitTestMode = false;
    private String unitTestModeHolder = "none";
    private ExecutorService unitTestRefreshThreadPool;

    private static final String DEFAULT_TARGET_CYCLE_DURATION_MILLIS_PROP =
            "UpdateGraphProcessor.targetCycleDurationMillis";
    private static final String MINIMUM_CYCLE_DURATION_TO_LOG_MILLIS_PROP =
            "UpdateGraphProcessor.minimumCycleDurationToLogMillis";
    private final long DEFAULT_TARGET_CYCLE_DURATION_MILLIS =
            Configuration.getInstance().getIntegerWithDefault(DEFAULT_TARGET_CYCLE_DURATION_MILLIS_PROP, 1000);
    private volatile long targetCycleDurationMillis = DEFAULT_TARGET_CYCLE_DURATION_MILLIS;
    private final long minimumCycleDurationToLogNanos = TimeUnit.MILLISECONDS.toNanos(
            Configuration.getInstance().getIntegerWithDefault(MINIMUM_CYCLE_DURATION_TO_LOG_MILLIS_PROP, 25));

    /**
     * How many cycles we have not logged, but were non-zero.
     */
    private long suppressedCycles = 0;
    private long suppressedCyclesTotalNanos = 0;

    /**
     * Accumulated UGP exclusive lock waits for the current cycle (or previous, if idle).
     */
    private long currentCycleLockWaitTotalNanos = 0;
    /**
     * Accumulated delays due to intracycle yields for the current cycle (or previous, if idle).
     */
    private long currentCycleYieldTotalNanos = 0L;
    /**
     * Accumulated delays due to intracycle sleeps for the current cycle (or previous, if idle).
     */

    private long currentCycleSleepTotalNanos = 0L;

    /**
     * Abstracts away the processing of non-terminal notifications.
     */
    private NotificationProcessor notificationProcessor;

    /**
     * The {@link LivenessScope} that should be on top of the {@link LivenessScopeStack} for all run and notification
     * processing. Only non-null while some thread is in {@link #doRefresh(Runnable)}.
     */
    private volatile LivenessScope refreshScope;

    /**
     * The number of threads in our executor service for dispatching notifications. If 1, then we don't actually use the
     * executor service; but instead dispatch all the notifications on the UpdateGraphProcessor run thread.
     */
    private final int updateThreads = Require.geq(
            Configuration.getInstance().getIntegerWithDefault("UpdateGraphProcessor.updateThreads", 1), "updateThreads",
            1);

    /**
     * Is this one of the threads engaged in notification processing? (Either the solitary run thread, or one of the
     * pooled threads it uses in some configurations)
     */
    private final ThreadLocal<Boolean> isRefreshThread = ThreadLocal.withInitial(() -> false);

    private final boolean CHECK_TABLE_OPERATIONS =
            Configuration.getInstance().getBooleanWithDefault("UpdateGraphProcessor.checkTableOperations", false);
    private final ThreadLocal<Boolean> checkTableOperations = ThreadLocal.withInitial(() -> CHECK_TABLE_OPERATIONS);

    private final long minimumInterCycleSleep =
            Configuration.getInstance().getIntegerWithDefault("UpdateGraphProcessor.minimumInterCycleSleep", 0);
    private final boolean interCycleYield =
            Configuration.getInstance().getBooleanWithDefault("UpdateGraphProcessor.interCycleYield", false);

    /**
     * Encapsulates locking support.
     */
    private final UpdateGraphLock lock = new UpdateGraphLock(LogicalClock.DEFAULT, allowUnitTestMode);

    /**
     * When UpdateGraphProcessor.printDependencyInformation is set to true, the UpdateGraphProcessor will print debug
     * information for each notification that has dependency information; as well as which notifications have been
     * completed and are outstanding.
     */
    private final boolean printDependencyInformation =
            Configuration.getInstance().getBooleanWithDefault("UpdateGraphProcessor.printDependencyInformation", false);

    UpdateGraphProcessor() {
        notificationProcessor = makeNotificationProcessor();

        refreshThread = new Thread("UpdateGraphProcessor." + name() + ".refreshThread") {
            @Override
            public void run() {
                configureRefreshThread();
                // noinspection InfiniteLoopStatement
                while (true) {
                    Assert.eqFalse(allowUnitTestMode, "allowUnitTestMode");
                    refreshTablesAndFlushNotifications();
                }
            }
        };
        refreshThread.setDaemon(true);
    }

    @Override
    public LogOutput append(@NotNull final LogOutput logOutput) {
        return logOutput.append("UpdateGraphProcessor-").append(name());
    }

    @Override
    public String toString() {
        return new LogOutputStringImpl().append(this).toString();
    }

    @NotNull
    private NotificationProcessor makeNotificationProcessor() {
        if (updateThreads > 1) {
            final ThreadFactory threadFactory = new UpdateGraphProcessorThreadFactory(
                    new ThreadGroup("UpdateGraphProcessor-updateExecutors"), "updateExecutor");
            return new ConcurrentNotificationProcessor(threadFactory, updateThreads);
        } else {
            return new QueueNotificationProcessor();
        }
    }

    @TestUseOnly
    private NotificationProcessor makeRandomizedNotificationProcessor(final Random random, final int nThreads,
            final int notificationStartDelay) {
        final UpdateGraphProcessorThreadFactory threadFactory = new UpdateGraphProcessorThreadFactory(
                new ThreadGroup("UpdateGraphProcessor-randomizedUpdatedExecutors"), "randomizedUpdateExecutor");
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
     * The UpdateGraphProcessor has a configurable number of update processing threads. The number of threads is exposed
     * in your method to enable you to partition a query based on the number of threads.
     * </p>
     *
     * @return the number of update threads configured.
     */
    @SuppressWarnings("unused")
    public int getUpdateThreads() {
        if (notificationProcessor == null) {
            return updateThreads;
        } else if (notificationProcessor instanceof ConcurrentNotificationProcessor) {
            return ((ConcurrentNotificationProcessor) notificationProcessor).threadCount();
        } else {
            return 1;
        }
    }

    // region Accessors for the shared and exclusive locks

    /**
     * <p>
     * Get the shared lock for this {@link UpdateGraphProcessor}.
     * <p>
     * Using this lock will prevent run processing from proceeding concurrently, but will allow other read-only
     * processing to proceed.
     * <p>
     * The shared lock implementation is expected to support reentrance.
     * <p>
     * This lock does <em>not</em> support {@link java.util.concurrent.locks.Lock#newCondition()}. Use the exclusive
     * lock if you need to wait on events that are driven by run processing.
     *
     * @return The shared lock for this {@link UpdateGraphProcessor}
     */
    public AwareFunctionalLock sharedLock() {
        return lock.sharedLock();
    }

    /**
     * <p>
     * Get the exclusive lock for this {@link UpdateGraphProcessor}.
     * <p>
     * Using this lock will prevent run or read-only processing from proceeding concurrently.
     * <p>
     * The exclusive lock implementation is expected to support reentrance.
     * <p>
     * Note that using the exclusive lock while the shared lock is held by the current thread will result in exceptions,
     * as lock upgrade is not supported.
     * <p>
     * This lock does support {@link java.util.concurrent.locks.Lock#newCondition()}.
     *
     * @return The exclusive lock for this {@link UpdateGraphProcessor}
     */
    public AwareFunctionalLock exclusiveLock() {
        return lock.exclusiveLock();
    }

    // endregion Accessors for the shared and exclusive locks

    /**
     * Test if this thread is part of our run thread executor service.
     *
     * @return whether this is one of our run threads.
     */
    public boolean isRefreshThread() {
        return isRefreshThread.get();
    }

    /**
     * <p>
     * If we are establishing a new table operation, on a refreshing table without the UpdateGraphProcessor lock; then
     * we are likely committing a grievous error, but one that will only occasionally result in us getting the wrong
     * answer or if we are lucky an assertion. This method is called from various query operations that should not be
     * established without the UGP lock.
     * </p>
     *
     * <p>
     * The run thread pool threads are allowed to instantiate operations, even though that thread does not have the
     * lock; because they are protected by the main run thread and dependency tracking.
     * </p>
     *
     * <p>
     * If you are sure that you know what you are doing better than the query engine, you may call
     * {@link #setCheckTableOperations(boolean)} to set a thread local variable bypassing this check.
     * </p>
     */
    public void checkInitiateTableOperation() {
        if (!getCheckTableOperations() || exclusiveLock().isHeldByCurrentThread()
                || sharedLock().isHeldByCurrentThread() || isRefreshThread()) {
            return;
        }
        throw new IllegalStateException(
                "May not initiate table operations: UGP exclusiveLockHeld=" + exclusiveLock().isHeldByCurrentThread()
                        + ", sharedLockHeld=" + sharedLock().isHeldByCurrentThread()
                        + ", refreshThread=" + isRefreshThread());
    }

    /**
     * If you know that the table operations you are performing are indeed safe, then call this method with false to
     * disable table operation checking. Conversely, if you want to enforce checking even if the configuration
     * disagrees; call it with true.
     *
     * @param value the new value of check table operations
     * @return the old value of check table operations
     */
    @SuppressWarnings("unused")
    public boolean setCheckTableOperations(boolean value) {
        final boolean old = checkTableOperations.get();
        checkTableOperations.set(value);
        return old;
    }


    /**
     * Execute the supplied code while table operations are unchecked.
     *
     * @param supplier the function to run
     * @return the result of supplier
     */
    @SuppressWarnings("unused")
    public <T> T doUnchecked(Supplier<T> supplier) {
        final boolean old = getCheckTableOperations();
        try {
            setCheckTableOperations(false);
            return supplier.get();
        } finally {
            setCheckTableOperations(old);
        }
    }

    /**
     * Execute the supplied code while table operations are unchecked.
     *
     * @param runnable the function to run
     */
    @SuppressWarnings("unused")
    public void doUnchecked(Runnable runnable) {
        final boolean old = getCheckTableOperations();
        try {
            setCheckTableOperations(false);
            runnable.run();
        } finally {
            setCheckTableOperations(old);
        }
    }

    /**
     * Should this thread check table operations for safety with respect to the update lock?
     * 
     * @return if we should check table operations.
     */
    public boolean getCheckTableOperations() {
        return checkTableOperations.get();
    }

    /**
     * <p>
     * Set the target duration of an update cycle, including the updating phase and the idle phase. This is also the
     * target interval between the start of one cycle and the start of the next.
     * <p>
     * Can be reset to default via {@link #resetCycleDuration()}.
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
    @SuppressWarnings("unused")
    public long getTargetCycleDurationMillis() {
        return targetCycleDurationMillis;
    }

    /**
     * Resets the run cycle time to the default target configured via the
     * {@value #DEFAULT_TARGET_CYCLE_DURATION_MILLIS_PROP} property.
     *
     * @implNote If the {@value #DEFAULT_TARGET_CYCLE_DURATION_MILLIS_PROP} property is not set, this value defaults to
     *           1000ms.
     */
    @SuppressWarnings("unused")
    public void resetCycleDuration() {
        targetCycleDurationMillis = DEFAULT_TARGET_CYCLE_DURATION_MILLIS;
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
            throw new IllegalStateException("UpdateGraphProcessor.allowUnitTestMode=false");
        }
        if (refreshThread.isAlive()) {
            throw new IllegalStateException("UpdateGraphProcessor.refreshThread is executing!");
        }
        assertLockAvailable("enabling unit test mode");
        unitTestMode = true;
        unitTestModeHolder = ExceptionUtils.getStackTrace(new Exception());
        unitTestRefreshThreadPool = makeUnitTestRefreshExecutor();
    }

    private void assertLockAvailable(@NotNull final String action) {
        if (!UpdateGraphProcessor.DEFAULT.exclusiveLock().tryLock()) {
            log.error().append("Lock is held when ").append(action).append(", with previous holder: ")
                    .append(unitTestModeHolder).endl();
            ThreadDump.threadDump(System.err);
            UpdateGraphLock.DebugAwareFunctionalLock lock =
                    (UpdateGraphLock.DebugAwareFunctionalLock) UpdateGraphProcessor.DEFAULT.exclusiveLock();
            throw new IllegalStateException(
                    "Lock is held when " + action + ", with previous holder: " + lock.getDebugMessage());
        }
        UpdateGraphProcessor.DEFAULT.exclusiveLock().unlock();
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

    public void requestSignal(Condition updateGraphProcessorCondition) {
        if (UpdateGraphProcessor.DEFAULT.exclusiveLock().isHeldByCurrentThread()) {
            updateGraphProcessorCondition.signalAll();
        } else {
            // terminal notifications always run on the UGP thread
            final Notification terminalNotification = new TerminalNotification() {
                @Override
                public void run() {
                    Assert.assertion(UpdateGraphProcessor.DEFAULT.exclusiveLock().isHeldByCurrentThread(),
                            "UpdateGraphProcessor.DEFAULT.isHeldByCurrentThread()");
                    updateGraphProcessorCondition.signalAll();
                }

                @Override
                public boolean mustExecuteWithUgpLock() {
                    return true;
                }

                @Override
                public LogOutput append(LogOutput output) {
                    return output.append("SignalNotification(")
                            .append(System.identityHashCode(updateGraphProcessorCondition)).append(")");
                }
            };
            synchronized (terminalNotifications) {
                terminalNotifications.offer(terminalNotification);
            }
        }
    }

    private class WatchdogJob extends TimedJob {
        @Override
        public void timedOut() {
            if (watchDogTimeoutProcedure != null) {
                watchDogTimeoutProcedure.accept(watchDogMillis);
            }
        }
    }

    /**
     * Start the table run thread.
     *
     * @implNote Must not be in {@link #enableUnitTestMode() unit test} mode.
     */
    public void start() {
        Assert.eqFalse(unitTestMode, "unitTestMode");
        Assert.eqFalse(allowUnitTestMode, "allowUnitTestMode");
        synchronized (refreshThread) {
            if (!refreshThread.isAlive()) {
                log.info().append("UpdateGraphProcessor starting with ").append(updateThreads)
                        .append(" notification processing threads").endl();
                refreshThread.start();
            }
        }
    }

    /**
     * Add a table to the list of tables to run and mark it as {@link DynamicNode#setRefreshing(boolean) refreshing} if
     * it was a {@link DynamicNode}.
     *
     * @implNote This will do nothing in {@link #enableUnitTestMode() unit test} mode other than mark the table as
     *           refreshing.
     * @param updateSource The table to be added to the run list
     */
    @Override
    public void addSource(@NotNull final Runnable updateSource) {
        if (updateSource instanceof DynamicNode) {
            ((DynamicNode) updateSource).setRefreshing(true);
        }

        if (!allowUnitTestMode) {
            // if we are in unit test mode we never want to start the UGP
            sources.add(updateSource);
            start();
        }
    }

    @Override
    public void removeSource(@NotNull final Runnable updateSource) {
        sources.remove(updateSource);
    }

    /**
     * Remove a collection of sources from the list of refreshing sources.
     *
     * @implNote This will <i>not</i> set the sources as {@link DynamicNode#setRefreshing(boolean) non-refreshing}.
     * @param sourcesToRemove The sources to remove from the list of refreshing sources
     */
    public void removeSources(final Collection<Runnable> sourcesToRemove) {
        sources.removeAll(sourcesToRemove);
    }

    /**
     * Return the number of valid sources.
     *
     * @return the number of valid sources
     */
    public int sourceCount() {
        return sources.size();
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
        if (notification.isTerminal()) {
            synchronized (terminalNotifications) {
                terminalNotifications.offer(notification);
            }
        } else {
            logDependencies().append(Thread.currentThread().getName()).append(": Adding notification ")
                    .append(notification).endl();
            synchronized (pendingNormalNotifications) {
                Assert.eq(LogicalClock.DEFAULT.currentState(), "LogicalClock.DEFAULT.currentState()",
                        LogicalClock.State.Updating, "LogicalClock.State.Updating");
                pendingNormalNotifications.offer(notification);
            }
            notificationProcessor.onNotificationAdded();
        }
    }

    @Override
    public boolean maybeAddNotification(@NotNull final Notification notification, final long deliveryStep) {
        if (notificationAdditionDelay > 0) {
            SleepUtil.sleep(notificationRandomizer.nextInt(notificationAdditionDelay));
        }
        if (notification.isTerminal()) {
            throw new IllegalArgumentException("Notification must not be terminal");
        }
        logDependencies().append(Thread.currentThread().getName()).append(": Adding notification ").append(notification)
                .append(" if step is ").append(deliveryStep).endl();
        final boolean added;
        synchronized (pendingNormalNotifications) {
            // Note that the clock is advanced to idle under the pendingNormalNotifications lock, after which point no
            // further normal notifications will be processed on this cycle.
            final long logicalClockValue = LogicalClock.DEFAULT.currentValue();
            if (LogicalClock.getState(logicalClockValue) == LogicalClock.State.Updating
                    && LogicalClock.getStep(logicalClockValue) == deliveryStep) {
                pendingNormalNotifications.offer(notification);
                added = true;
            } else {
                added = false;
            }
        }
        if (added) {
            notificationProcessor.onNotificationAdded();
        }
        return added;
    }

    @Override
    public boolean satisfied(final long step) {
        return sourcesLastSatisfiedStep == step;
    }

    /**
     * Enqueue a collection of notifications to be flushed.
     *
     * @param notifications The notification to enqueue
     *
     * @see #addNotification(Notification)
     */
    public void addNotifications(@NotNull final Collection<Notification> notifications) {
        synchronized (pendingNormalNotifications) {
            synchronized (terminalNotifications) {
                notifications.forEach(this::addNotification);
            }
        }
    }

    /**
     * Request that the next update cycle begin as soon as practicable. This "hurry-up" cycle happens through normal
     * means using the refresh thread and its workers.
     */
    @Override
    public void requestRefresh() {
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
    public void resetForUnitTests(boolean after,
            final boolean randomizedNotifications, final int seed, final int maxRandomizedThreadCount,
            final int notificationStartDelay, final int notificationAdditionDelay) {
        final List<String> errors = new ArrayList<>();
        this.notificationRandomizer = new Random(seed);
        this.notificationAdditionDelay = notificationAdditionDelay;
        Assert.assertion(unitTestMode, "unitTestMode");
        sources.clear();
        notificationProcessor.shutdown();
        synchronized (pendingNormalNotifications) {
            pendingNormalNotifications.clear();
        }
        isRefreshThread.remove();
        if (randomizedNotifications) {
            notificationProcessor = makeRandomizedNotificationProcessor(notificationRandomizer,
                    maxRandomizedThreadCount, notificationStartDelay);
        } else {
            notificationProcessor = makeNotificationProcessor();
        }
        synchronized (terminalNotifications) {
            terminalNotifications.clear();
        }
        LogicalClock.DEFAULT.resetForUnitTests();
        sourcesLastSatisfiedStep = LogicalClock.DEFAULT.currentStep();

        refreshScope = null;
        if (after) {
            LivenessManager stackTop;
            while ((stackTop = LivenessScopeStack.peek()) instanceof LivenessScope) {
                LivenessScopeStack.pop((LivenessScope) stackTop);
            }
            CleanupReferenceProcessorInstance.resetAllForUnitTests();
        }

        ensureUnlocked("unit test reset thread", errors);

        if (refreshThread.isAlive()) {
            errors.add("UGP refreshThread isAlive");
        }

        try {
            unitTestRefreshThreadPool.submit(() -> ensureUnlocked("unit test run pool thread", errors)).get();
        } catch (InterruptedException | ExecutionException e) {
            errors.add("Failed to ensure UGP unlocked from unit test run thread pool: " + e);
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
            final String message = "UGP reset for unit tests reported errors:\n\t" + String.join("\n\t", errors);
            System.err.println(message);
            if (after) {
                throw new IllegalStateException(message);
            }
        }

        assertLockAvailable("resetting for unit tests");
    }

    /**
     * Begin the next {@link LogicalClock#startUpdateCycle() update cycle} while in {@link #enableUnitTestMode()
     * unit-test} mode. Note that this happens on a simulated UGP run thread, rather than this thread.
     */
    @TestUseOnly
    public void startCycleForUnitTests() {
        Assert.assertion(unitTestMode, "unitTestMode");
        try {
            unitTestRefreshThreadPool.submit(this::startCycleForUnitTestsInternal).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new UncheckedDeephavenException(e);
        }
    }

    @TestUseOnly
    private void startCycleForUnitTestsInternal() {
        // noinspection AutoBoxing
        isRefreshThread.set(true);
        UpdateGraphProcessor.DEFAULT.exclusiveLock().lock();

        Assert.eqNull(refreshScope, "refreshScope");
        refreshScope = new LivenessScope();
        LivenessScopeStack.push(refreshScope);

        LogicalClock.DEFAULT.startUpdateCycle();
        sourcesLastSatisfiedStep = LogicalClock.DEFAULT.currentStep();
    }

    /**
     * Do the second half of the update cycle, including flushing notifications, and completing the
     * {@link LogicalClock#completeUpdateCycle() LogicalClock} update cycle. Note that this happens on a simulated UGP
     * run thread, rather than this thread.
     */
    @TestUseOnly
    public void completeCycleForUnitTests() {
        Assert.assertion(unitTestMode, "unitTestMode");
        try {
            unitTestRefreshThreadPool.submit(this::completeCycleForUnitTestsInternal).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new UncheckedDeephavenException(e);
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

            UpdateGraphProcessor.DEFAULT.exclusiveLock().unlock();
            isRefreshThread.remove();
        }) {
            flushNotificationsAndCompleteCycle();
        }
    }

    /**
     * Execute the given runnable wrapped with {@link #startCycleForUnitTests()} and
     * {@link #completeCycleForUnitTests()}. Note that the runnable is run on the current thread.
     *
     * @param runnable the runnable to execute.
     */
    @TestUseOnly
    public <T extends Exception> void runWithinUnitTestCycle(FunctionalInterfaces.ThrowingRunnable<T> runnable)
            throws T {
        startCycleForUnitTests();
        try {
            runnable.run();
        } finally {
            completeCycleForUnitTests();
        }
    }

    /**
     * Refresh an update source on a simulated UGP run thread, rather than this thread.
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
     * Flush a single notification from the UGP queue. Note that this happens on a simulated UGP run thread, rather than
     * this thread.
     *
     * @return whether a notification was found in the queue
     */
    @TestUseOnly
    public boolean flushOneNotificationForUnitTests() {
        Assert.assertion(unitTestMode, "unitTestMode");

        final NotificationProcessor existingNotificationProcessor = notificationProcessor;
        try {
            this.notificationProcessor = new ControlledNotificationProcessor();
            // noinspection AutoUnboxing,AutoBoxing
            return unitTestRefreshThreadPool.submit(this::flushOneNotificationForUnitTestsInternal).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new UncheckedDeephavenException(e);
        } finally {
            this.notificationProcessor = existingNotificationProcessor;
        }
    }

    @TestUseOnly
    public boolean flushOneNotificationForUnitTestsInternal() {
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
            Assert.eqFalse(notification.mustExecuteWithUgpLock(), "notification.mustExecuteWithUgpLock()");

            if (notification.canExecute(LogicalClock.DEFAULT.currentStep())) {
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
        } else if (somethingWasPending) {
            Assert.statementNeverExecuted(
                    "Did not flush any notifications in unit test mode, yet there were outstanding notifications");
        }
        return satisfied != null;
    }

    /**
     * Flush all the normal notifications from the UGP queue. Note that the flushing happens on a simulated UGP run
     * thread, rather than this thread.
     */
    @TestUseOnly
    public void flushAllNormalNotificationsForUnitTests() {
        flushAllNormalNotificationsForUnitTests(() -> true, 0).run();
    }

    /**
     * Flush all the normal notifications from the UGP queue, continuing until {@code done} returns {@code true}. Note
     * that the flushing happens on a simulated UGP run thread, rather than this thread.
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
            final long deadlineNanoTime = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
            boolean flushed;
            while ((flushed = flushOneNotificationForUnitTestsInternal()) || !done.getAsBoolean()) {
                if (!flushed) {
                    final long remainingNanos = deadlineNanoTime - System.nanoTime();
                    if (!controlledNotificationProcessor.blockUntilNotificationAdded(remainingNanos)) {
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
     * If the run thread is waiting in {@link #flushNormalNotificationsAndCompleteCycle()} or
     * {@link #flushAllNormalNotificationsForUnitTests(BooleanSupplier, long)}, wake it up.
     */
    @TestUseOnly
    public void wakeRefreshThreadForUnitTests() {
        // Pretend we may have added a notification
        notificationProcessor.onNotificationAdded();
    }

    /**
     * Flush all non-terminal notifications, complete the logical clock update cycle, then flush all terminal
     * notifications.
     */
    private void flushNotificationsAndCompleteCycle() {
        // We cannot proceed with normal notifications, nor are we satisfied, until all update source refresh
        // notifications
        // have been processed. Note that non-update source notifications that require dependency satisfaction are
        // delivered first to the pendingNormalNotifications queue, and hence will not be processed until we advance to
        // the flush* methods.
        // TODO: If and when we properly integrate update sources into the dependency tracking system, we can
        // discontinue this distinct phase, along with the requirement to treat the UGP itself as a Dependency.
        // Until then, we must delay the beginning of "normal" notification processing until all update sources are
        // done. See IDS-8039.
        notificationProcessor.doAllWork();
        sourcesLastSatisfiedStep = LogicalClock.DEFAULT.currentStep();

        flushNormalNotificationsAndCompleteCycle();
        flushTerminalNotifications();
        synchronized (pendingNormalNotifications) {
            Assert.assertion(pendingNormalNotifications.isEmpty(), "pendingNormalNotifications.isEmpty()");
        }
    }

    /**
     * Flush all non-terminal {@link Notification notifications} from the queue.
     */
    private void flushNormalNotificationsAndCompleteCycle() {
        final IntrusiveDoublyLinkedQueue<Notification> pendingToEvaluate =
                new IntrusiveDoublyLinkedQueue<>(IntrusiveDoublyLinkedNode.Adapter.<Notification>getInstance());
        while (true) {
            final int outstandingCountAtStart = notificationProcessor.outstandingNotificationsCount();
            notificationProcessor.beforeNotificationsDrained();
            synchronized (pendingNormalNotifications) {
                pendingToEvaluate.transferAfterTailFrom(pendingNormalNotifications);
                if (outstandingCountAtStart == 0 && pendingToEvaluate.isEmpty()) {
                    // We complete the cycle here before releasing the lock on pendingNotifications, so that
                    // maybeAddNotification can detect scenarios where the notification cannot be delivered on the
                    // desired step.
                    LogicalClock.DEFAULT.completeUpdateCycle();
                    break;
                }
            }
            logDependencies().append(Thread.currentThread().getName())
                    .append(": Notification queue size=").append(pendingToEvaluate.size())
                    .append(", outstanding=").append(outstandingCountAtStart)
                    .endl();

            boolean nothingBecameSatisfied = true;
            for (final Iterator<Notification> it = pendingToEvaluate.iterator(); it.hasNext();) {
                final Notification notification = it.next();

                Assert.eqFalse(notification.isTerminal(), "notification.isTerminal()");
                Assert.eqFalse(notification.mustExecuteWithUgpLock(), "notification.mustExecuteWithUgpLock()");

                final boolean satisfied = notification.canExecute(sourcesLastSatisfiedStep);
                if (satisfied) {
                    nothingBecameSatisfied = false;
                    it.remove();
                    logDependencies().append(Thread.currentThread().getName())
                            .append(": Submitting to notification processor ").append(notification).endl();
                    notificationProcessor.submit(notification);
                } else {
                    logDependencies().append(Thread.currentThread().getName()).append(": Unmet dependencies for ")
                            .append(notification).endl();
                }
            }
            if (outstandingCountAtStart == 0 && nothingBecameSatisfied) {
                throw new IllegalStateException(
                        "No outstanding notifications, yet the notification queue is not empty!");
            }
            if (notificationProcessor.outstandingNotificationsCount() > 0) {
                notificationProcessor.doWork();
            }
        }
        synchronized (pendingNormalNotifications) {
            Assert.eqZero(pendingNormalNotifications.size() + pendingToEvaluate.size(),
                    "pendingNormalNotifications.size() + pendingToEvaluate.size()");
        }
    }

    /**
     * Flush all {@link Notification#isTerminal() terminal} {@link Notification notifications} from the queue.
     *
     * @implNote Any notification that may have been queued while the clock's state is Updating must be invoked during
     *           this cycle's Idle phase.
     */
    private void flushTerminalNotifications() {
        synchronized (terminalNotifications) {
            for (final Iterator<Notification> it = terminalNotifications.iterator(); it.hasNext();) {
                final Notification notification = it.next();
                Assert.assertion(notification.isTerminal(), "notification.isTerminal()");

                if (!notification.mustExecuteWithUgpLock()) {
                    it.remove();
                    // for the single threaded queue case; this enqueues the notification;
                    // for the executor service case, this causes the notification to be kicked off
                    notificationProcessor.submit(notification);
                }
            }
        }

        // run the notifications that must be run on this thread
        while (true) {
            final Notification notificationForThisThread;
            synchronized (terminalNotifications) {
                notificationForThisThread = terminalNotifications.poll();
            }
            if (notificationForThisThread == null) {
                break;
            }
            runNotification(notificationForThisThread);
        }

        // We can not proceed until all of the terminal notifications have executed.
        notificationProcessor.doAllWork();
    }

    /**
     * Abstract away the details of satisfied notification processing.
     */
    private interface NotificationProcessor {

        /**
         * Submit a satisfied notification for processing.
         *
         * @param notification The notification
         */
        void submit(@NotNull NotificationQueue.Notification notification);

        /**
         * Submit a queue of satisfied notification for processing.
         *
         * @param notifications The queue of notifications to
         *        {@link IntrusiveDoublyLinkedQueue#transferAfterTailFrom(IntrusiveDoublyLinkedQueue) transfer} from.
         *        Will become empty as a result of successful completion
         */
        void submitAll(@NotNull IntrusiveDoublyLinkedQueue<NotificationQueue.Notification> notifications);

        /**
         * Query the number of outstanding notifications submitted to this processor.
         *
         * @return The number of outstanding notifications
         */
        int outstandingNotificationsCount();

        /**
         * <p>
         * Do work (or in the multi-threaded case, wait for some work to have happened).
         * <p>
         * Caller must know that work is outstanding.
         */
        void doWork();

        /**
         * Do all outstanding work.
         */
        void doAllWork();

        /**
         * Shutdown this notification processor (for unit tests).
         */
        void shutdown();

        /**
         * Called after a pending notification is added.
         */
        void onNotificationAdded();

        /**
         * Called before pending notifications are drained.
         */
        void beforeNotificationsDrained();
    }

    private void runNotification(@NotNull final Notification notification) {
        logDependencies().append(Thread.currentThread().getName()).append(": Executing ").append(notification).endl();

        final LivenessScope scope;
        final boolean releaseScopeOnClose;
        if (notification.isTerminal()) {
            // Terminal notifications can't create new notifications, so they have no need to participate in a shared
            // run scope.
            scope = new LivenessScope();
            releaseScopeOnClose = true;
        } else {
            // Non-terminal notifications must use a shared run scope.
            Assert.neqNull(refreshScope, "refreshScope");
            scope = refreshScope == LivenessScopeStack.peek() ? null : refreshScope;
            releaseScopeOnClose = false;
        }

        try (final SafeCloseable ignored = scope == null ? null : LivenessScopeStack.open(scope, releaseScopeOnClose)) {
            notification.run();
            logDependencies().append(Thread.currentThread().getName()).append(": Completed ").append(notification)
                    .endl();
        } catch (final Exception e) {
            log.error().append(Thread.currentThread().getName())
                    .append(": Exception while executing UpdateGraphProcessor notification: ").append(notification)
                    .append(": ").append(e).endl();
            ProcessEnvironment.getGlobalFatalErrorReporter()
                    .report("Exception while processing UpdateGraphProcessor notification", e);
        }
    }

    private class ConcurrentNotificationProcessor implements NotificationProcessor {

        private final IntrusiveDoublyLinkedQueue<Notification> satisfiedNotifications =
                new IntrusiveDoublyLinkedQueue<>(IntrusiveDoublyLinkedNode.Adapter.<Notification>getInstance());
        private final Thread[] updateThreads;

        private final AtomicInteger outstandingNotifications = new AtomicInteger(0);
        private final Semaphore pendingNormalNotificationsCheckNeeded = new Semaphore(0, false);

        private volatile boolean running = true;

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
            while (running) {
                Notification satisfiedNotification = null;
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
                try {
                    runNotification(satisfiedNotification);
                } finally {
                    outstandingNotifications.decrementAndGet();
                    pendingNormalNotificationsCheckNeeded.release();
                }
            }
            log.info().append(Thread.currentThread().getName()).append(": terminating");
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
            } catch (InterruptedException ignored) {
            }
        }

        @Override
        public void doAllWork() {
            while (outstandingNotificationsCount() > 0) {
                doWork();
            }
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

    private class QueueNotificationProcessor implements NotificationProcessor {

        final IntrusiveDoublyLinkedQueue<Notification> satisfiedNotifications =
                new IntrusiveDoublyLinkedQueue<>(IntrusiveDoublyLinkedNode.Adapter.<Notification>getInstance());

        @Override
        public void submit(@NotNull final Notification notification) {
            satisfiedNotifications.offer(notification);
        }

        @Override
        public void submitAll(@NotNull IntrusiveDoublyLinkedQueue<Notification> notifications) {
            satisfiedNotifications.transferAfterTailFrom(notifications);
        }

        @Override
        public int outstandingNotificationsCount() {
            return satisfiedNotifications.size();
        }

        @Override
        public void doWork() {
            Notification satisfiedNotification;
            while ((satisfiedNotification = satisfiedNotifications.poll()) != null) {
                runNotification(satisfiedNotification);
            }
        }

        @Override
        public void doAllWork() {
            doWork();
        }

        @Override
        public void shutdown() {
            satisfiedNotifications.clear();
        }

        @Override
        public void onNotificationAdded() {}

        @Override
        public void beforeNotificationsDrained() {}
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
            Assert.statementNeverExecuted();
        }

        @Override
        public void doAllWork() {
            Assert.statementNeverExecuted();
        }

        @Override
        public void shutdown() {
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
                Assert.statementNeverExecuted();
                return false;
            }
        }
    }

    /**
     * Iterate over all monitored tables and run them. This method also ensures that the loop runs no faster than
     * {@link #getTargetCycleDurationMillis() minimum cycle time}.
     */
    private void refreshTablesAndFlushNotifications() {
        final Scheduler sched = CommBase.getScheduler();
        final long startTime = sched.currentTimeMillis();
        final long startTimeNanos = System.nanoTime();

        if (sources.isEmpty()) {
            exclusiveLock().doLocked(this::flushTerminalNotifications);
        } else {
            currentCycleLockWaitTotalNanos = currentCycleYieldTotalNanos = currentCycleSleepTotalNanos = 0L;

            WatchdogJob watchdogJob = null;

            if ((watchDogMillis > 0) && (watchDogTimeoutProcedure != null)) {
                watchdogJob = new WatchdogJob();
                sched.installJob(watchdogJob, startTime + watchDogMillis);
            }

            refreshAllTables();

            if (watchdogJob != null) {
                sched.cancelJob(watchdogJob);
            }
            final long cycleTime = System.nanoTime() - startTimeNanos;
            if (cycleTime >= minimumCycleDurationToLogNanos) {
                if (suppressedCycles > 0) {
                    logSuppressedCycles();
                }
                log.info().append("Update Graph Processor cycleTime=").appendDouble(cycleTime / 1_000_000.0)
                        .append("ms, lockWaitTime=").appendDouble(currentCycleLockWaitTotalNanos / 1_000_000.0)
                        .append("ms, yieldTime=").appendDouble(currentCycleYieldTotalNanos / 1_000_000.0)
                        .append("ms, sleepTime=").appendDouble(currentCycleSleepTotalNanos / 1_000_000.0)
                        .append("ms").endl();
            } else if (cycleTime > 0) {
                suppressedCycles++;
                suppressedCyclesTotalNanos += cycleTime;
                if (suppressedCyclesTotalNanos >= minimumCycleDurationToLogNanos) {
                    logSuppressedCycles();
                }
            }
        }

        if (interCycleYield) {
            Thread.yield();
        }

        waitForNextCycle(startTime, sched);
    }

    private void logSuppressedCycles() {
        log.info().append("Minimal Update Graph Processor cycle times: ")
                .appendDouble((double) (suppressedCyclesTotalNanos) / 1_000_000.0).append("ms / ")
                .append(suppressedCycles).append(" cycles = ")
                .appendDouble((double) suppressedCyclesTotalNanos / (double) suppressedCycles / 1_000_000.0)
                .append("ms/cycle average").endl();
        suppressedCycles = suppressedCyclesTotalNanos = 0;
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
     * @param startTime The start time of the last run cycle
     * @param timeSource The source of time that startTime was based on
     */
    private void waitForNextCycle(final long startTime, final Scheduler timeSource) {
        long expectedEndTime = startTime + targetCycleDurationMillis;
        if (minimumInterCycleSleep > 0) {
            expectedEndTime = Math.max(expectedEndTime, timeSource.currentTimeMillis() + minimumInterCycleSleep);
        }
        waitForEndTime(expectedEndTime, timeSource);
    }

    /**
     * <p>
     * Ensure the current time is past {@code expectedEndTime} before returning, or return early if an immediate refresh
     * is requested.
     * <p>
     * If the delay is interrupted for any other {@link InterruptedException reason}, it will be logged and continue to
     * wait the remaining period.
     *
     * @param expectedEndTime The time which we should sleep until
     * @param timeSource The source of time that startTime was based on
     */
    private void waitForEndTime(final long expectedEndTime, final Scheduler timeSource) {
        long remainingMillis;
        while ((remainingMillis = expectedEndTime - timeSource.currentTimeMillis()) > 0) {
            if (refreshRequested.get()) {
                return;
            }
            synchronized (refreshRequested) {
                if (refreshRequested.get()) {
                    return;
                }
                try {
                    refreshRequested.wait(remainingMillis);
                } catch (final InterruptedException logAndIgnore) {
                    log.warn().append("Interrupted while waiting on refreshRequested. Ignoring: ").append(logAndIgnore)
                            .endl();
                }
            }
        }
    }

    /**
     * Refresh all the update sources within an {@link LogicalClock update cycle} after the UGP has been locked. At the
     * end of the updates all {@link Notification notifications} will be flushed.
     */
    private void refreshAllTables() {
        refreshRequested.set(false);
        doRefresh(() -> sources.forEach((final UpdateSourceRefreshNotification updateSourceNotification,
                final Runnable unused) -> notificationProcessor.submit(updateSourceNotification)));
    }

    /**
     * Perform a run cycle, using {@code refreshFunction} to ensure the desired update sources are refreshed at the
     * start.
     *
     * @param refreshFunction Function to submit one or more {@link UpdateSourceRefreshNotification update source
     *        refresh notifications} to the {@link NotificationProcessor notification processor} or run them directly.
     */
    private void doRefresh(@NotNull final Runnable refreshFunction) {
        final long lockStartTimeNanos = System.nanoTime();
        exclusiveLock().doLocked(() -> {
            currentCycleLockWaitTotalNanos += System.nanoTime() - lockStartTimeNanos;
            synchronized (pendingNormalNotifications) {
                Assert.eqZero(pendingNormalNotifications.size(), "pendingNormalNotifications.size()");
            }
            Assert.eqNull(refreshScope, "refreshScope");
            refreshScope = new LivenessScope();
            final long updatingCycleValue = LogicalClock.DEFAULT.startUpdateCycle();
            try (final SafeCloseable ignored = LivenessScopeStack.open(refreshScope, true)) {
                refreshFunction.run();
                flushNotificationsAndCompleteCycle();
            } finally {
                LogicalClock.DEFAULT.ensureUpdateCycleCompleted(updatingCycleValue);
                refreshScope = null;
            }
        });
    }

    /**
     * Re-usable class for adapting update sources to {@link Notification}s.
     */
    private static final class UpdateSourceRefreshNotification extends AbstractNotification
            implements SimpleReference<Runnable> {

        private final WeakReference<Runnable> updateSourceRef;

        private UpdateSourceRefreshNotification(@NotNull final Runnable updateSource) {
            super(false);
            updateSourceRef = new WeakReference<>(updateSource);
        }

        @Override
        public LogOutput append(@NotNull final LogOutput logOutput) {
            return logOutput.append("UpdateSourceRefreshNotification{").append(System.identityHashCode(this))
                    .append(", for UpdateSource{").append(System.identityHashCode(get())).append("}}");
        }

        @Override
        public boolean canExecute(final long step) {
            return true;
        }

        @Override
        public void run() {
            final Runnable updateSource = updateSourceRef.get();
            if (updateSource == null) {
                return;
            }
            updateSource.run();
        }

        @Override
        public Runnable get() {
            // NB: Arguably we should make get() and clear() synchronized.
            return updateSourceRef.get();
        }

        @Override
        public void clear() {
            updateSourceRef.clear();
        }
    }

    public LogEntry logDependencies() {
        if (printDependencyInformation) {
            return log.info();
        } else {
            return LogEntry.NULL;
        }
    }

    private class UpdateGraphProcessorThreadFactory extends NamingThreadFactory {
        private UpdateGraphProcessorThreadFactory(@NotNull final ThreadGroup threadGroup, @NotNull final String name) {
            super(threadGroup, UpdateGraphProcessor.class, name, true);
        }

        @Override
        public Thread newThread(@NotNull final Runnable r) {
            return super.newThread(() -> {
                configureRefreshThread();
                r.run();
            });
        }
    }

    @TestUseOnly
    private void ensureUnlocked(@NotNull final String callerDescription, @Nullable final List<String> errors) {
        if (exclusiveLock().isHeldByCurrentThread()) {
            if (errors != null) {
                errors.add(callerDescription + ": UGP exclusive lock is still held");
            }
            while (exclusiveLock().isHeldByCurrentThread()) {
                exclusiveLock().unlock();
            }
        }
        if (sharedLock().isHeldByCurrentThread()) {
            if (errors != null) {
                errors.add(callerDescription + ": UGP shared lock is still held");
            }
            while (sharedLock().isHeldByCurrentThread()) {
                sharedLock().unlock();
            }
        }
    }

    private ExecutorService makeUnitTestRefreshExecutor() {
        return Executors.newFixedThreadPool(1, new UnitTestRefreshThreadFactory());
    }

    @TestUseOnly
    private class UnitTestRefreshThreadFactory extends NamingThreadFactory {

        private UnitTestRefreshThreadFactory() {
            super(UpdateGraphProcessor.class, "unitTestRefresh", true);
        }

        @Override
        public Thread newThread(@NotNull final Runnable runnable) {
            final Thread thread = super.newThread(runnable);
            final Thread.UncaughtExceptionHandler existing = thread.getUncaughtExceptionHandler();
            thread.setUncaughtExceptionHandler((final Thread errorThread, final Throwable throwable) -> {
                ensureUnlocked("unit test run pool thread exception handler", null);
                existing.uncaughtException(errorThread, throwable);
            });
            return thread;
        }
    }

    /**
     * Configure the primary UGP thread or one of the auxiliary run threads.
     */
    private void configureRefreshThread() {
        SystemicObjectTracker.markThreadSystemic();
        MultiChunkPool.enableDedicatedPoolForThisThread();
        isRefreshThread.set(true);
    }
}
