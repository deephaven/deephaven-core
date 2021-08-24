/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.live;

import io.deephaven.base.SleepUtil;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.reference.SimpleReference;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.io.log.LogEntry;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.io.logger.Logger;
import io.deephaven.io.sched.Scheduler;
import io.deephaven.io.sched.TimedJob;
import io.deephaven.net.CommBase;
import io.deephaven.util.process.ProcessEnvironment;
import io.deephaven.db.v2.utils.AbstractNotification;
import io.deephaven.util.datastructures.SimpleReferenceManager;
import io.deephaven.util.datastructures.intrusive.IntrusiveArraySet;
import io.deephaven.util.thread.ThreadDump;
import io.deephaven.db.tables.utils.SystemicObjectTracker;
import io.deephaven.db.util.liveness.LivenessManager;
import io.deephaven.db.util.liveness.LivenessScope;
import io.deephaven.db.util.liveness.LivenessScopeStack;
import io.deephaven.db.util.reference.CleanupReferenceProcessorInstance;
import io.deephaven.db.v2.DynamicNode;
import io.deephaven.db.v2.InstrumentedListener;
import io.deephaven.db.v2.sources.LogicalClock;
import io.deephaven.db.v2.sources.chunk.util.pools.MultiChunkPool;
import io.deephaven.db.v2.utils.TerminalNotification;
import io.deephaven.util.FunctionalInterfaces;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.TestUseOnly;
import io.deephaven.util.datastructures.linked.IntrusiveDoublyLinkedNode;
import io.deephaven.util.datastructures.linked.IntrusiveDoublyLinkedQueue;
import io.deephaven.util.locks.AwareFunctionalLock;
import io.deephaven.util.thread.NamingThreadFactory;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.UncheckedDeephavenException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.WeakReference;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

/**
 * <p>
 * This class contains a thread which periodically updates a set of monitored {@link LiveTable
 * LiveTables} at a specified target cycle time. The target cycle time can be
 * {@link #setTargetCycleTime(long) configured} to reduce or increase the refresh rate of the
 * monitored tables.
 * </p>
 *
 * <p>
 * This class can be configured via the following {@link Configuration} property
 * </p>
 * <ul>
 * <li><b>LiveTableMonitor.targetcycletime</b> <i>(optional)</i> - The default target cycle time in
 * ms (1000 if not defined)</li>
 * </ul>
 */
public enum LiveTableMonitor
    implements LiveTableRegistrar, NotificationQueue, NotificationQueue.Dependency {
    DEFAULT;

    private final Logger log = LoggerFactory.getLogger(LiveTableMonitor.class);

    /**
     * {@link LiveTable}s that are part of this LiveTableMonitor.
     */
    private final SimpleReferenceManager<LiveTable, LiveTableRefreshNotification> tables =
        new SimpleReferenceManager<>(LiveTableRefreshNotification::new);

    /**
     * Recorder for live table satisfaction as a phase of notification processing.
     */
    private volatile long tablesLastSatisfiedStep;

    /**
     * The queue of non-terminal notifications to process.
     */
    private final IntrusiveDoublyLinkedQueue<Notification> pendingNormalNotifications =
        new IntrusiveDoublyLinkedQueue<>(
            IntrusiveDoublyLinkedNode.Adapter.<Notification>getInstance());

    /**
     * The queue of terminal notifications to process.
     */
    private final IntrusiveDoublyLinkedQueue<Notification> terminalNotifications =
        new IntrusiveDoublyLinkedQueue<>(
            IntrusiveDoublyLinkedNode.Adapter.<Notification>getInstance());

    // Specifically not using a ConcurrentListDeque here because we don't want to create wasteful
    // garbage
    // when we know this collection is going to constantly grow and shrink.
    private final IntrusiveArraySet<LiveTableRefreshNotification> singleUpdateQueue =
        new IntrusiveArraySet<>(SingleUpdateSlotAdapter.INSTANCE,
            LiveTableRefreshNotification.class);

    private final Thread refreshThread;

    /**
     * If this is set to a positive value, then we will call the {@link #watchDogTimeoutProcedure}
     * if any single refresh loop takes longer than this value. The intention is to use this for
     * strategies, or other queries, where a LiveTableMonitor loop that is "stuck" is the equivalent
     * of an error. Set the value with {@link #setWatchDogMillis(int)}.
     */
    private int watchDogMillis = 0;
    /**
     * If a timeout time has been {@link #setWatchDogMillis(int) set}, this procedure will be called
     * if any single refresh loop takes longer than the value specified. Set the value with
     * {@link #setWatchDogTimeoutProcedure(LongConsumer)}.
     */
    private LongConsumer watchDogTimeoutProcedure = null;

    private final boolean allowUnitTestMode = Configuration.getInstance()
        .getBooleanWithDefault("LiveTableMonitor.allowUnitTestMode", false);
    private int notificationAdditionDelay = 0;
    private Random notificationRandomizer = new Random(0);
    private boolean unitTestMode = false;
    private String unitTestModeHolder = "none";
    private ExecutorService unitTestRefreshThreadPool;

    private final long defaultTargetCycleTime =
        Configuration.getInstance().getIntegerWithDefault("LiveTableMonitor.targetcycletime", 1000);
    private volatile long targetCycleTime = defaultTargetCycleTime;
    private final long minimumCycleLogNanos = TimeUnit.MILLISECONDS.toNanos(Configuration
        .getInstance().getIntegerWithDefault("LiveTableMonitor.minimumCycleLogTime", 25));

    /**
     * How many cycles we have not logged, but were non-zero.
     */
    private long suppressedCycles = 0;
    private long suppressedCyclesTotalNanos = 0;

    /**
     * Accumulated LTM exclusive lock waits for the current cycle (or previous, if idle).
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
     * The {@link LivenessScope} that should be on top of the {@link LivenessScopeStack} for all
     * refresh and notification processing. Only non-null while some thread is in
     * {@link #doRefresh(Runnable)}.
     */
    private volatile LivenessScope refreshScope;

    /**
     * The number of threads in our executor service for dispatching notifications. If 1, then we
     * don't actually use the executor service; but instead dispatch all the notifications on the
     * LiveTableMonitor refresh thread.
     */
    private final int updateThreads = Require.geq(
        Configuration.getInstance().getIntegerWithDefault("LiveTableMonitor.updateThreads", 1),
        "updateThreads", 1);

    /**
     * Is this one of the threads engaged in notification processing? (Either the solitary refresh
     * thread, or one of the pooled threads it uses in some configurations)
     */
    private final ThreadLocal<Boolean> isRefreshThread = ThreadLocal.withInitial(() -> false);

    private final boolean CHECK_TABLE_OPERATIONS = Configuration.getInstance()
        .getBooleanWithDefault("LiveTableMonitor.checkTableOperations", false);
    private final ThreadLocal<Boolean> checkTableOperations =
        ThreadLocal.withInitial(() -> CHECK_TABLE_OPERATIONS);

    private final long minimumInterCycleSleep = Configuration.getInstance()
        .getIntegerWithDefault("LiveTableMonitor.minimumInterCycleSleep", 0);
    private final boolean interCycleYield = Configuration.getInstance()
        .getBooleanWithDefault("LiveTableMonitor.interCycleYield", false);

    /**
     * Encapsulates locking support.
     */
    private final LiveTableMonitorLock lock =
        new LiveTableMonitorLock(LogicalClock.DEFAULT, allowUnitTestMode);

    /**
     * When LiveTableMonitor.printDependencyInformation is set to true, the LiveTableMonitor will
     * print debug information for each notification that has dependency information; as well as
     * which notifications have been completed and are outstanding.
     */
    private final boolean printDependencyInformation = Configuration.getInstance()
        .getBooleanWithDefault("LiveTableMonitor.printDependencyInformation", false);

    LiveTableMonitor() {
        notificationProcessor = makeNotificationProcessor();

        refreshThread = new Thread("LiveTableMonitor." + name() + ".refreshThread") {
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
        return logOutput.append("LiveTableMonitor-").append(name());
    }

    @Override
    public String toString() {
        return new LogOutputStringImpl().append(this).toString();
    }

    @NotNull
    private NotificationProcessor makeNotificationProcessor() {
        if (updateThreads > 1) {
            final ThreadFactory threadFactory = new LiveTableMonitorThreadFactory(
                new ThreadGroup("LiveTableMonitor-updateExecutors"), "updateExecutor");
            return new ConcurrentNotificationProcessor(threadFactory, updateThreads);
        } else {
            return new QueueNotificationProcessor();
        }
    }

    @TestUseOnly
    private NotificationProcessor makeRandomizedNotificationProcessor(final Random random,
        final int nThreads, final int notificationStartDelay) {
        final LiveTableMonitorThreadFactory threadFactory = new LiveTableMonitorThreadFactory(
            new ThreadGroup("LiveTableMonitor-randomizedUpdatedExecutors"),
            "randomizedUpdateExecutor");
        return new ConcurrentNotificationProcessor(threadFactory, nThreads) {

            private Notification addRandomDelay(@NotNull final Notification notification) {
                if (notificationStartDelay <= 0) {
                    return notification;
                }
                return new NotificationWrapper(notification) {
                    @Override
                    public void run() {
                        final int millis = random.nextInt(notificationStartDelay);
                        logDependencies().append(Thread.currentThread().getName())
                            .append(": Sleeping for  ").append(millis).append("ms").endl();
                        SleepUtil.sleep(millis);
                        super.run();
                    }
                };
            }

            @Override
            public void submit(@NotNull Notification notification) {
                if (notification instanceof LiveTableRefreshNotification) {
                    super.submit(notification);
                } else if (notification instanceof InstrumentedListener.ErrorNotification) {
                    // NB: The previous implementation of this concept was more rigorous about
                    // ensuring that errors
                    // would be next, but this is likely good enough.
                    submitAt(notification, 0);
                } else {
                    submitAt(addRandomDelay(notification),
                        random.nextInt(outstandingNotificationsCount() + 1));
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
     * The LiveTableMonitor has a configurable number of update processing threads. The number of
     * threads is exposed in your method to enable you to partition a query based on the number of
     * threads.
     * </p>
     *
     * @return the number of update threads configured.
     */
    @SuppressWarnings("unused")
    public int getUpdateThreads() {
        return updateThreads;
    }

    // region Accessors for the shared and exclusive locks

    /**
     * <p>
     * Get the shared lock for this {@link LiveTableMonitor}.
     * <p>
     * Using this lock will prevent refresh processing from proceeding concurrently, but will allow
     * other read-only processing to proceed.
     * <p>
     * The shared lock implementation is expected to support reentrance.
     * <p>
     * This lock does <em>not</em> support {@link java.util.concurrent.locks.Lock#newCondition()}.
     * Use the exclusive lock if you need to wait on events that are driven by refresh processing.
     *
     * @return The shared lock for this {@link LiveTableMonitor}
     */
    public AwareFunctionalLock sharedLock() {
        return lock.sharedLock();
    }

    /**
     * <p>
     * Get the exclusive lock for this {@link LiveTableMonitor}.
     * <p>
     * Using this lock will prevent refresh or read-only processing from proceeding concurrently.
     * <p>
     * The exclusive lock implementation is expected to support reentrance.
     * <p>
     * Note that using the exclusive lock while the shared lock is held by the current thread will
     * result in exceptions, as lock upgrade is not supported.
     * <p>
     * This lock does support {@link java.util.concurrent.locks.Lock#newCondition()}.
     *
     * @return The exclusive lock for this {@link LiveTableMonitor}
     */
    public AwareFunctionalLock exclusiveLock() {
        return lock.exclusiveLock();
    }

    // endregion Accessors for the shared and exclusive locks

    /**
     * Test if this thread is part of our refresh thread executor service.
     *
     * @return whether this is one of our refresh threads.
     */
    public boolean isRefreshThread() {
        return isRefreshThread.get();
    }

    /**
     * <p>
     * If we are establishing a new table operation, on a refreshing table without the
     * LiveTableMonitor lock; then we are likely committing a grievous error, but one that will only
     * occasionally result in us getting the wrong answer or if we are lucky an assertion. This
     * method is called from various query operations that should not be established without the LTM
     * lock.
     * </p>
     *
     * <p>
     * The refresh thread pool threads are allowed to instantiate operations, even though that
     * thread does not have the lock; because they are protected by the main refresh thread and
     * dependency tracking.
     * </p>
     *
     * <p>
     * If you are sure that you know what you are doing better than the query engine, you may call
     * {@link #setCheckTableOperations(boolean)} to set a thread local variable bypassing this
     * check.
     * </p>
     */
    public void checkInitiateTableOperation() {
        if (!getCheckTableOperations() || exclusiveLock().isHeldByCurrentThread()
            || sharedLock().isHeldByCurrentThread() || isRefreshThread()) {
            return;
        }
        throw new IllegalStateException("May not initiate table operations: LTM exclusiveLockHeld="
            + exclusiveLock().isHeldByCurrentThread()
            + ", sharedLockHeld=" + sharedLock().isHeldByCurrentThread()
            + ", refreshThread=" + isRefreshThread());
    }

    /**
     * If you know that the table operations you are performing are indeed safe, then call this
     * method with false to disable table operation checking. Conversely, if you want to enforce
     * checking even if the configuration disagrees; call it with true.
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
     * Should this thread check table operations?
     * 
     * @return if we should check table operations.
     */
    public boolean getCheckTableOperations() {
        return checkTableOperations.get();
    }

    /**
     * <p>
     * Set the target clock time between refresh cycles.
     * </p>
     *
     * <p>
     * Can be reset to default via {@link #resetCycleTime()}
     * </p>
     *
     * @implNote Any cycle time < 0 will be clamped to 0.
     *
     * @param cycleTime The target time between refreshes in milliseconds
     */
    public void setTargetCycleTime(long cycleTime) {
        targetCycleTime = Math.max(cycleTime, 0);
    }

    /**
     * Get the target period between refresh cycles.
     *
     * @return The {@link #setTargetCycleTime(long) current} minimum cycle time
     */
    @SuppressWarnings("unused")
    public long getTargetCycleTime() {
        return targetCycleTime;
    }

    /**
     * Resets the refresh cycle time to the default target configured via the
     * LiveTableMonitor.targetcycletime property.
     *
     * @implNote If the LiveTableMonitor.targetcycletime property is not set, this value defaults to
     *           1000ms.
     */
    @SuppressWarnings("unused")
    public void resetCycleTime() {
        targetCycleTime = defaultTargetCycleTime;
    }

    /**
     * <p>
     * Enable unit test mode.
     * </p>
     *
     * <p>
     * In this mode calls to {@link #addTable(LiveTable)} will only mark tables as
     * {@link DynamicNode#setRefreshing(boolean) refreshing}. Additionally {@link #start()} may not
     * be called.
     * </p>
     */
    public void enableUnitTestMode() {
        if (unitTestMode) {
            return;
        }
        if (!allowUnitTestMode) {
            throw new IllegalStateException("LiveTableMonitor.allowUnitTestMode=false");
        }
        if (refreshThread.isAlive()) {
            throw new IllegalStateException("LiveTableMonitor.refreshThread is executing!");
        }
        assertLockAvailable("enabling unit test mode");
        unitTestMode = true;
        unitTestModeHolder = ExceptionUtils.getStackTrace(new Exception());
        unitTestRefreshThreadPool = makeUnitTestRefreshExecutor();
    }

    private void assertLockAvailable(@NotNull final String action) {
        if (!LiveTableMonitor.DEFAULT.exclusiveLock().tryLock()) {
            log.error().append("Lock is held when ").append(action)
                .append(", with previous holder: ").append(unitTestModeHolder).endl();
            ThreadDump.threadDump(System.err);
            LiveTableMonitorLock.DebugAwareFunctionalLock lock =
                (LiveTableMonitorLock.DebugAwareFunctionalLock) LiveTableMonitor.DEFAULT
                    .exclusiveLock();
            throw new IllegalStateException("Lock is held when " + action
                + ", with previous holder: " + lock.getDebugMessage());
        }
        LiveTableMonitor.DEFAULT.exclusiveLock().unlock();
    }

    /**
     * Enable the loop watchdog with the specified timeout. A value of 0 disables the watchdog.
     *
     * @implNote Any timeout < 0 will be clamped to 0.
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

    public void requestSignal(Condition liveTableMonitorCondition) {
        if (LiveTableMonitor.DEFAULT.exclusiveLock().isHeldByCurrentThread()) {
            liveTableMonitorCondition.signalAll();
        } else {
            // terminal notifications always run on the LTM thread
            final Notification terminalNotification = new TerminalNotification() {
                @Override
                public void run() {
                    Assert.assertion(
                        LiveTableMonitor.DEFAULT.exclusiveLock().isHeldByCurrentThread(),
                        "LiveTableMonitor.DEFAULT.isHeldByCurrentThread()");
                    liveTableMonitorCondition.signalAll();
                }

                @Override
                public boolean mustExecuteWithLtmLock() {
                    return true;
                }

                @Override
                public LogOutput append(LogOutput output) {
                    return output.append("SignalNotification(")
                        .append(System.identityHashCode(liveTableMonitorCondition)).append(")");
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
     * Start the table refresh thread.
     *
     * @implNote Must not be in {@link #enableUnitTestMode() unit test} mode.
     */
    public void start() {
        Assert.eqFalse(unitTestMode, "unitTestMode");
        Assert.eqFalse(allowUnitTestMode, "allowUnitTestMode");
        synchronized (refreshThread) {
            if (!refreshThread.isAlive()) {
                log.info().append("LiveTableMonitor starting with ").append(updateThreads)
                    .append(" notification processing threads").endl();
                refreshThread.start();
            }
        }
    }

    /**
     * Add a table to the list of tables to refresh and mark it as
     * {@link DynamicNode#setRefreshing(boolean) refreshing} if it was a {@link DynamicNode}.
     *
     * @implNote This will do nothing in {@link #enableUnitTestMode() unit test} mode other than
     *           mark the table as refreshing.
     * @param table The table to be added to the refresh list
     */
    @Override
    public void addTable(@NotNull final LiveTable table) {
        if (table instanceof DynamicNode) {
            ((DynamicNode) table).setRefreshing(true);
        }

        if (!allowUnitTestMode) {
            // if we are in unit test mode we never want to start the LTM
            tables.add(table);
            start();
        }
    }

    @Override
    public void removeTable(@NotNull final LiveTable liveTable) {
        tables.remove(liveTable);
    }

    /**
     * Remove a collection of tables from the list of refreshing tables.
     *
     * @implNote This will <i>not</i> set the tables as {@link DynamicNode#setRefreshing(boolean)
     *           non-refreshing}.
     * @param tablesToRemove The tables to remove from the list of refreshing tables
     */
    public void removeTables(final Collection<LiveTable> tablesToRemove) {
        tables.removeAll(tablesToRemove);
    }

    /**
     * Enqueue a notification to be flushed according to its priority. Non-terminal notifications
     * should only be enqueued during the updating phase of a cycle. That is, they should be
     * enqueued from a {@link LiveTable#refresh()} or subsequent notification delivery.
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
            logDependencies().append(Thread.currentThread().getName())
                .append(": Adding notification ").append(notification).endl();
            synchronized (pendingNormalNotifications) {
                Assert.eq(LogicalClock.DEFAULT.currentState(),
                    "LogicalClock.DEFAULT.currentState()", LogicalClock.State.Updating,
                    "LogicalClock.State.Updating");
                pendingNormalNotifications.offer(notification);
            }
            notificationProcessor.onNotificationAdded();
        }
    }

    @Override
    public boolean maybeAddNotification(@NotNull final Notification notification,
        final long deliveryStep) {
        if (notificationAdditionDelay > 0) {
            SleepUtil.sleep(notificationRandomizer.nextInt(notificationAdditionDelay));
        }
        if (notification.isTerminal()) {
            throw new IllegalArgumentException("Notification must not be terminal");
        }
        logDependencies().append(Thread.currentThread().getName()).append(": Adding notification ")
            .append(notification).append(" if step is ").append(deliveryStep).endl();
        final boolean added;
        synchronized (pendingNormalNotifications) {
            // Note that the clock is advanced to idle under the pendingNormalNotifications lock,
            // after which point no
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
        return tablesLastSatisfiedStep == step;
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
     * Refresh {@code liveTable} on this thread if it is registered.
     *
     * @param liveTable The {@link LiveTable} that we would like to refresh
     */
    private void maybeRefreshTable(@NotNull final LiveTable liveTable) {
        final LiveTableRefreshNotification liveTableRefreshNotification =
            tables.getFirstReference((final LiveTable found) -> found == liveTable);
        if (liveTableRefreshNotification == null) {
            return;
        }
        refreshOneTable(liveTableRefreshNotification);
    }

    /**
     * Acquire the exclusive lock if necessary and do a refresh of {@code liveTable} on this thread
     * if it is registered with this LTM.
     *
     * @param liveTable The {@link LiveTable} that we would like to refresh
     * @param onlyIfHaveLock If true, check that the lock is held first and do nothing if it is not
     */
    @Override
    public void maybeRefreshTable(@NotNull final LiveTable liveTable,
        final boolean onlyIfHaveLock) {
        if (!onlyIfHaveLock || exclusiveLock().isHeldByCurrentThread()) {
            maybeRefreshTable(liveTable);
        }
    }

    /**
     * <p>
     * Request a refresh for a single {@link LiveTable live table}, which must already be registered
     * with this LiveTableMonitor.
     * </p>
     * <p>
     * The update will occur on the LTM thread, but will not necessarily wait for the next scheduled
     * cycle.
     * </p>
     *
     * @param liveTable The {@link LiveTable live table} to refresh
     */
    @Override
    public void requestRefresh(@NotNull final LiveTable liveTable) {
        final LiveTableRefreshNotification liveTableRefreshNotification =
            tables.getFirstReference((final LiveTable found) -> found == liveTable);
        if (liveTableRefreshNotification == null) {
            return;
        }
        synchronized (singleUpdateQueue) {
            singleUpdateQueue.add(liveTableRefreshNotification);
            singleUpdateQueue.notify();
        }
    }

    /**
     * Clear all monitored tables and enqueued notifications to support {@link #enableUnitTestMode()
     * unit-tests}.
     *
     * @param after Whether this is *after* a unit test completed. If true, held locks should result
     *        in an exception and the LivenessScopeStack will be cleared.
     */
    @TestUseOnly
    public void resetForUnitTests(final boolean after) {
        resetForUnitTests(after, false, 0, 0, 0, 0);
    }

    /**
     * Clear all monitored tables and enqueued notifications to support {@link #enableUnitTestMode()
     * unit-tests}.
     *
     * @param after Whether this is *after* a unit test completed. If true, held locks should result
     *        in an exception and the LivenessScopeStack will be cleared.
     * @param randomizedNotifications Whether the notification processor should randomize the order
     *        of delivery
     * @param seed Seed for randomized notification delivery order and delays
     * @param maxRandomizedThreadCount Maximum number of threads handling randomized notification
     *        delivery
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
        tables.clear();
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
        tablesLastSatisfiedStep = LogicalClock.DEFAULT.currentStep();

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
            errors.add("LTM refreshThread isAlive");
        }

        try {
            unitTestRefreshThreadPool
                .submit(() -> ensureUnlocked("unit test refresh pool thread", errors)).get();
        } catch (InterruptedException | ExecutionException e) {
            errors.add("Failed to ensure LTM unlocked from unit test refresh thread pool: "
                + e.toString());
        }
        unitTestRefreshThreadPool.shutdownNow();
        try {
            if (!unitTestRefreshThreadPool.awaitTermination(1, TimeUnit.SECONDS)) {
                errors.add("Failed to cleanup jobs in unit test refresh thread pool");
            }
        } catch (InterruptedException e) {
            errors.add("Interrupted while trying to cleanup jobs in unit test refresh thread pool");
        }
        unitTestRefreshThreadPool = makeUnitTestRefreshExecutor();

        if (!errors.isEmpty()) {
            final String message =
                "LTM reset for unit tests reported errors:\n\t" + String.join("\n\t", errors);
            System.err.println(message);
            if (after) {
                throw new IllegalStateException(message);
            }
        }

        assertLockAvailable("resetting for unit tests");
    }

    /**
     * Begin the next {@link LogicalClock#startUpdateCycle() update cycle} while in
     * {@link #enableUnitTestMode() unit-test} mode. Note that this happens on a simulated LTM
     * refresh thread, rather than this thread.
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
        LiveTableMonitor.DEFAULT.exclusiveLock().lock();

        Assert.eqNull(refreshScope, "refreshScope");
        refreshScope = new LivenessScope();
        LivenessScopeStack.push(refreshScope);

        LogicalClock.DEFAULT.startUpdateCycle();
        tablesLastSatisfiedStep = LogicalClock.DEFAULT.currentStep();
    }

    /**
     * Do the second half of the update cycle, including flushing notifications, and completing the
     * {@link LogicalClock#completeUpdateCycle() LogicalClock} update cycle. Note that this happens
     * on a simulated LTM refresh thread, rather than this thread.
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

            LiveTableMonitor.DEFAULT.exclusiveLock().unlock();
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
    public <T extends Exception> void runWithinUnitTestCycle(
        FunctionalInterfaces.ThrowingRunnable<T> runnable) throws T {
        startCycleForUnitTests();
        try {
            runnable.run();
        } finally {
            completeCycleForUnitTests();
        }
    }

    /**
     * Refresh a {@link LiveTable} on a simulated LTM refresh thread, rather than this thread.
     *
     * @param liveTable The {@link LiveTable} to refresh
     */
    @TestUseOnly
    public void refreshLiveTableForUnitTests(@NotNull final LiveTable liveTable) {
        Assert.assertion(unitTestMode, "unitTestMode");
        try {
            unitTestRefreshThreadPool.submit(liveTable::refresh).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new UncheckedDeephavenException(e);
        }
    }

    /**
     * Flush a single notification from the LTM queue. Note that this happens on a simulated LTM
     * refresh thread, rather than this thread.
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
            return unitTestRefreshThreadPool.submit(this::flushOneNotificationForUnitTestsInternal)
                .get();
        } catch (InterruptedException | ExecutionException e) {
            throw new UncheckedDeephavenException(e);
        } finally {
            this.notificationProcessor = existingNotificationProcessor;
        }
    }

    @TestUseOnly
    public boolean flushOneNotificationForUnitTestsInternal() {
        final IntrusiveDoublyLinkedQueue<Notification> pendingToEvaluate =
            new IntrusiveDoublyLinkedQueue<>(
                IntrusiveDoublyLinkedNode.Adapter.<Notification>getInstance());
        notificationProcessor.beforeNotificationsDrained();
        synchronized (pendingNormalNotifications) {
            pendingToEvaluate.transferAfterTailFrom(pendingNormalNotifications);
        }
        final boolean somethingWasPending = !pendingToEvaluate.isEmpty();
        Notification satisfied = null;
        for (final Iterator<Notification> it = pendingToEvaluate.iterator(); it.hasNext();) {
            final Notification notification = it.next();

            Assert.eqFalse(notification.isTerminal(), "notification.isTerminal()");
            Assert.eqFalse(notification.mustExecuteWithLtmLock(),
                "notification.mustExecuteWithLtmLock()");

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
     * Flush all the normal notifications from the LTM queue. Note that the flushing happens on a
     * simulated LTM refresh thread, rather than this thread.
     */
    @TestUseOnly
    public void flushAllNormalNotificationsForUnitTests() {
        flushAllNormalNotificationsForUnitTests(() -> true, 0).run();
    }

    /**
     * Flush all the normal notifications from the LTM queue, continuing until {@code done} returns
     * {@code true}. Note that the flushing happens on a simulated LTM refresh thread, rather than
     * this thread.
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
        final ControlledNotificationProcessor controlledNotificationProcessor =
            new ControlledNotificationProcessor();
        notificationProcessor = controlledNotificationProcessor;
        final Future<?> flushJobFuture = unitTestRefreshThreadPool.submit(() -> {
            final long deadlineNanoTime =
                System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
            boolean flushed;
            while ((flushed = flushOneNotificationForUnitTestsInternal()) || !done.getAsBoolean()) {
                if (!flushed) {
                    final long remainingNanos = deadlineNanoTime - System.nanoTime();
                    if (!controlledNotificationProcessor
                        .blockUntilNotificationAdded(remainingNanos)) {
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
     * If the refresh thread is waiting in {@link #flushNormalNotificationsAndCompleteCycle()} or
     * {@link #flushAllNormalNotificationsForUnitTests(BooleanSupplier, long)}, wake it up.
     */
    @TestUseOnly
    public void wakeRefreshThreadForUnitTests() {
        // Pretend we may have added a notification
        notificationProcessor.onNotificationAdded();
    }

    /**
     * Flush all non-terminal notifications, complete the logical clock update cycle, then flush all
     * terminal notifications.
     */
    private void flushNotificationsAndCompleteCycle() {
        // We cannot proceed with normal notifications, nor are we satisfied, until all LiveTable
        // refresh notifications
        // have been processed. Note that non-LiveTable notifications that require dependency
        // satisfaction are delivered
        // first to the pendingNormalNotifications queue, and hence will not be processed until we
        // advance to the flush*
        // methods.
        // TODO: If and when we properly integrate LiveTables into the dependency tracking system,
        // we can
        // discontinue this distinct phase, along with the requirement to treat the LTM itself as a
        // Dependency.
        // Until then, we must delay the beginning of "normal" notification processing until all
        // LiveTables are
        // done. See IDS-8039.
        notificationProcessor.doAllWork();
        tablesLastSatisfiedStep = LogicalClock.DEFAULT.currentStep();

        flushNormalNotificationsAndCompleteCycle();
        flushTerminalNotifications();
        synchronized (pendingNormalNotifications) {
            Assert.assertion(pendingNormalNotifications.isEmpty(),
                "pendingNormalNotifications.isEmpty()");
        }
    }

    /**
     * Flush all non-terminal {@link Notification notifications} from the queue.
     */
    private void flushNormalNotificationsAndCompleteCycle() {
        final IntrusiveDoublyLinkedQueue<Notification> pendingToEvaluate =
            new IntrusiveDoublyLinkedQueue<>(
                IntrusiveDoublyLinkedNode.Adapter.<Notification>getInstance());
        while (true) {
            final int outstandingCountAtStart =
                notificationProcessor.outstandingNotificationsCount();
            notificationProcessor.beforeNotificationsDrained();
            synchronized (pendingNormalNotifications) {
                pendingToEvaluate.transferAfterTailFrom(pendingNormalNotifications);
                if (outstandingCountAtStart == 0 && pendingToEvaluate.isEmpty()) {
                    // We complete the cycle here before releasing the lock on pendingNotifications,
                    // so that
                    // maybeAddNotification can detect scenarios where the notification cannot be
                    // delivered on the
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
                Assert.eqFalse(notification.mustExecuteWithLtmLock(),
                    "notification.mustExecuteWithLtmLock()");

                final boolean satisfied = notification.canExecute(tablesLastSatisfiedStep);
                if (satisfied) {
                    nothingBecameSatisfied = false;
                    it.remove();
                    logDependencies().append(Thread.currentThread().getName())
                        .append(": Submitting to notification processor ").append(notification)
                        .endl();
                    notificationProcessor.submit(notification);
                } else {
                    logDependencies().append(Thread.currentThread().getName())
                        .append(": Unmet dependencies for ").append(notification).endl();
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
     * Flush all {@link Notification#isTerminal() terminal} {@link Notification notifications} from
     * the queue.
     *
     * @implNote Any notification that may have been queued while the clock's state is Updating must
     *           be invoked during this cycle's Idle phase.
     */
    private void flushTerminalNotifications() {
        synchronized (terminalNotifications) {
            for (final Iterator<Notification> it = terminalNotifications.iterator(); it
                .hasNext();) {
                final Notification notification = it.next();
                Assert.assertion(notification.isTerminal(), "notification.isTerminal()");

                if (!notification.mustExecuteWithLtmLock()) {
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
         *        {@link IntrusiveDoublyLinkedQueue#transferAfterTailFrom(IntrusiveDoublyLinkedQueue)
         *        transfer} from. Will become empty as a result of successful completion
         */
        void submitAll(
            @NotNull IntrusiveDoublyLinkedQueue<NotificationQueue.Notification> notifications);

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
        logDependencies().append(Thread.currentThread().getName()).append(": Executing ")
            .append(notification).endl();

        final LivenessScope scope;
        final boolean releaseScopeOnClose;
        if (notification.isTerminal()) {
            // Terminal notifications can't create new notifications, so they have no need to
            // participate in a shared refresh scope.
            scope = new LivenessScope();
            releaseScopeOnClose = true;
        } else {
            // Non-terminal notifications must use a shared refresh scope.
            Assert.neqNull(refreshScope, "refreshScope");
            scope = refreshScope == LivenessScopeStack.peek() ? null : refreshScope;
            releaseScopeOnClose = false;
        }

        try (final SafeCloseable ignored =
            scope == null ? null : LivenessScopeStack.open(scope, releaseScopeOnClose)) {
            notification.run();
            logDependencies().append(Thread.currentThread().getName()).append(": Completed ")
                .append(notification).endl();
        } catch (final Exception e) {
            log.error().append(Thread.currentThread().getName())
                .append(": Exception while executing LiveTableMonitor notification: ")
                .append(notification).append(": ").append(e).endl();
            ProcessEnvironment.getGlobalFatalErrorReporter()
                .report("Exception while processing LiveTableMonitor notification", e);
        }
    }

    private class ConcurrentNotificationProcessor implements NotificationProcessor {

        private final IntrusiveDoublyLinkedQueue<Notification> satisfiedNotifications =
            new IntrusiveDoublyLinkedQueue<>(
                IntrusiveDoublyLinkedNode.Adapter.<Notification>getInstance());
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
                    while (running
                        && (satisfiedNotification = satisfiedNotifications.poll()) == null) {
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
                // We clamp the size here because there's a race between the random offset selection
                // and other threads
                // draining the queue of satisfied notifications.
                satisfiedNotifications.insert(notification,
                    Math.min(offset, satisfiedNotifications.size()));
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
    }

    private class QueueNotificationProcessor implements NotificationProcessor {

        final IntrusiveDoublyLinkedQueue<Notification> satisfiedNotifications =
            new IntrusiveDoublyLinkedQueue<>(
                IntrusiveDoublyLinkedNode.Adapter.<Notification>getInstance());

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
        public void submitAll(
            @NotNull final IntrusiveDoublyLinkedQueue<Notification> notifications) {
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
                return pendingNormalNotificationsCheckNeeded.tryAcquire(nanosToWait,
                    TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                Assert.statementNeverExecuted();
                return false;
            }
        }
    }

    /**
     * Iterate over all monitored tables and refresh them. This method also ensures that the loop
     * runs no faster than {@link #getTargetCycleTime() minimum cycle time}.
     */
    private void refreshTablesAndFlushNotifications() {
        final Scheduler sched = CommBase.getScheduler();
        final long startTime = sched.currentTimeMillis();
        final long startTimeNanos = System.nanoTime();

        if (tables.isEmpty()) {
            exclusiveLock().doLocked(this::flushTerminalNotifications);
        } else {
            currentCycleLockWaitTotalNanos =
                currentCycleYieldTotalNanos = currentCycleSleepTotalNanos = 0L;

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
            if (cycleTime >= minimumCycleLogNanos) {
                if (suppressedCycles > 0) {
                    logSuppressedCycles();
                }
                log.info().append("Live Table Monitor cycleTime=")
                    .appendDouble(cycleTime / 1_000_000.0)
                    .append("ms, lockWaitTime=")
                    .appendDouble(currentCycleLockWaitTotalNanos / 1_000_000.0)
                    .append("ms, yieldTime=")
                    .appendDouble(currentCycleYieldTotalNanos / 1_000_000.0)
                    .append("ms, sleepTime=")
                    .appendDouble(currentCycleSleepTotalNanos / 1_000_000.0)
                    .append("ms").endl();
            } else if (cycleTime > 0) {
                suppressedCycles++;
                suppressedCyclesTotalNanos += cycleTime;
                if (suppressedCyclesTotalNanos >= minimumCycleLogNanos) {
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
        log.info().append("Minimal Live Table Monitor cycle times: ")
            .appendDouble((double) (suppressedCyclesTotalNanos) / 1_000_000.0).append("ms / ")
            .append(suppressedCycles).append(" cycles = ")
            .appendDouble(
                (double) suppressedCyclesTotalNanos / (double) suppressedCycles / 1_000_000.0)
            .append("ms/cycle average").endl();
        suppressedCycles = suppressedCyclesTotalNanos = 0;
    }

    /**
     * <p>
     * Ensure that at least {@link #getTargetCycleTime() minCycleTime} has passed before returning.
     * </p>
     *
     * <p>
     * If the delay is interrupted by a {@link #requestRefresh(LiveTable) request} to refresh a
     * single table this task will drain the queue of single refresh requests, then continue to wait
     * for a complete period if necessary.
     * </p>
     *
     * <p>
     * If the delay is interrupted for any other {@link InterruptedException reason}, it will be
     * logged and continue to wait the remaining period.
     * </p>
     *
     * @param startTime The start time of the last refresh cycle
     * @param timeSource The source of time that startTime was based on
     */
    private void waitForNextCycle(final long startTime, final Scheduler timeSource) {
        long expectedEndTime = startTime + targetCycleTime;
        if (minimumInterCycleSleep > 0) {
            expectedEndTime =
                Math.max(expectedEndTime, timeSource.currentTimeMillis() + minimumInterCycleSleep);
        }
        waitForEndTime(expectedEndTime, timeSource);
    }

    /**
     * <p>
     * Ensure the current time is past expectedEndTime before returning.
     * </p>
     *
     * <p>
     * If the delay is interrupted by a {@link #requestRefresh(LiveTable) request} to refresh a
     * single table this task will drain the queue of single refresh requests, then continue to wait
     * for a complete period if necessary.
     * </p>
     *
     * <p>
     * If the delay is interrupted for any other {@link InterruptedException reason}, it will be
     * logged and continue to wait the remaining period.
     * </p>
     *
     * @param expectedEndTime The time which we should sleep until
     * @param timeSource The source of time that startTime was based on
     */
    private void waitForEndTime(final long expectedEndTime, final Scheduler timeSource) {
        while (timeSource.currentTimeMillis() < expectedEndTime) {
            drainSingleUpdateQueue();

            final long currentDelay = expectedEndTime - timeSource.currentTimeMillis();
            if (currentDelay <= 0) {
                return;
            }

            try {
                synchronized (singleUpdateQueue) {
                    if (singleUpdateQueue.isEmpty()) {
                        singleUpdateQueue.wait(currentDelay);
                    }
                }
            } catch (InterruptedException logAndIgnore) {
                log.warn().append("Interrupted while waiting on singleUpdateQueue.  Ignoring: ")
                    .append(logAndIgnore).endl();
            }
        }
    }

    /**
     * <p>
     * Drain the single update queue from the front and refresh each of the tables.
     * </p>
     *
     * @implNote If the table is not monitored by the LTM it may not be refreshed
     */
    private void drainSingleUpdateQueue() {
        // NB: This is called while we're waiting for the next LTM cycle to start, thus blocking the
        // next cycle even
        // though it doesn't hold the LTM lock. The only race is with single updates, which are not
        // submitted to
        // the notification processor (and hence have no conflict over next/prev pointers).
        final IntrusiveDoublyLinkedQueue<Notification> liveTableNotifications;
        synchronized (singleUpdateQueue) {
            if (singleUpdateQueue.isEmpty()) {
                return;
            }
            liveTableNotifications = new IntrusiveDoublyLinkedQueue<>(
                IntrusiveDoublyLinkedNode.Adapter.<Notification>getInstance());
            singleUpdateQueue.forEach(liveTableNotifications::offer);
            singleUpdateQueue.clear();
        }
        doRefresh(() -> notificationProcessor.submitAll(liveTableNotifications));
    }

    /**
     * Refresh a single {@link LiveTable live table} within an {@link LogicalClock update cycle}
     * after the LTM has been locked. At the end of the update all {@link Notification
     * notifications} will be flushed.
     *
     * @param liveTableNotification The enclosing notification for the {@link LiveTable} to refresh
     */
    private void refreshOneTable(
        @NotNull final LiveTableRefreshNotification liveTableNotification) {
        // We're refreshing this table already, we should not prioritize its refresh again.
        synchronized (singleUpdateQueue) {
            singleUpdateQueue.removeIf(
                (final LiveTableRefreshNotification found) -> found == liveTableNotification);
        }
        doRefresh(() -> runNotification(liveTableNotification));
    }

    /**
     * Refresh all the {@link LiveTable live tables} within an {@link LogicalClock update cycle}
     * after the LTM has been locked. At the end of the updates all {@link Notification
     * notifications} will be flushed.
     */
    private void refreshAllTables() {
        // We're refreshing all tables already, we should not prioritize refresh for any of them
        // again.
        synchronized (singleUpdateQueue) {
            singleUpdateQueue.clear();
        }
        doRefresh(() -> tables.forEach((final LiveTableRefreshNotification liveTableNotification,
            final LiveTable unused) -> notificationProcessor.submit(liveTableNotification)));
    }

    /**
     * Perform a refresh cycle, using {@code refreshFunction} to ensure the desired {@link LiveTable
     * live tables} are refreshed at the start.
     *
     * @param refreshFunction Function to submit one or more {@link LiveTableRefreshNotification
     *        live table refresh notifications} to the {@link NotificationProcessor notification
     *        processor} or run them directly.
     */
    private void doRefresh(@NotNull final Runnable refreshFunction) {
        final long lockStartTimeNanos = System.nanoTime();
        exclusiveLock().doLocked(() -> {
            currentCycleLockWaitTotalNanos += System.nanoTime() - lockStartTimeNanos;
            synchronized (pendingNormalNotifications) {
                Assert.eqZero(pendingNormalNotifications.size(),
                    "pendingNormalNotifications.size()");
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
     * Re-usable class for adapting {@link LiveTable}s to {@link Notification}s.
     */
    private static final class LiveTableRefreshNotification extends AbstractNotification
        implements SimpleReference<LiveTable> {

        private final WeakReference<LiveTable> liveTableRef;

        private int singleUpdateQueueSlot;

        private LiveTableRefreshNotification(@NotNull final LiveTable liveTable) {
            super(false);
            liveTableRef = new WeakReference<>(liveTable);
        }

        @Override
        public LogOutput append(@NotNull final LogOutput logOutput) {
            return logOutput.append("LiveTableRefreshNotification{")
                .append(System.identityHashCode(this))
                .append(", for LiveTable{").append(System.identityHashCode(get())).append("}}");
        }

        @Override
        public boolean canExecute(final long step) {
            return true;
        }

        @Override
        public void run() {
            final LiveTable liveTable = liveTableRef.get();
            if (liveTable == null) {
                return;
            }
            liveTable.refresh();
        }

        @Override
        public LiveTable get() {
            // NB: Arguably we should make get() and clear() synchronized.
            return liveTableRef.get();
        }

        @Override
        public void clear() {
            liveTableRef.clear();
        }
    }

    private static final class SingleUpdateSlotAdapter
        implements IntrusiveArraySet.Adapter<LiveTableRefreshNotification> {

        private static final IntrusiveArraySet.Adapter<LiveTableRefreshNotification> INSTANCE =
            new SingleUpdateSlotAdapter();

        private SingleUpdateSlotAdapter() {}

        @Override
        public int getSlot(@NotNull final LiveTableRefreshNotification element) {
            return element.singleUpdateQueueSlot;
        }

        @Override
        public void setSlot(@NotNull final LiveTableRefreshNotification element, final int slot) {
            element.singleUpdateQueueSlot = slot;
        }
    }

    public LogEntry logDependencies() {
        if (printDependencyInformation) {
            return log.info();
        } else {
            return LogEntry.NULL;
        }
    }

    private class LiveTableMonitorThreadFactory extends NamingThreadFactory {
        private LiveTableMonitorThreadFactory(@NotNull final ThreadGroup threadGroup,
            @NotNull final String name) {
            super(threadGroup, LiveTableMonitor.class, name, true);
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
    private void ensureUnlocked(@NotNull final String callerDescription,
        @Nullable final List<String> errors) {
        if (exclusiveLock().isHeldByCurrentThread()) {
            if (errors != null) {
                errors.add(callerDescription + ": LTM exclusive lock is still held");
            }
            while (exclusiveLock().isHeldByCurrentThread()) {
                exclusiveLock().unlock();
            }
        }
        if (sharedLock().isHeldByCurrentThread()) {
            if (errors != null) {
                errors.add(callerDescription + ": LTM shared lock is still held");
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
            super(LiveTableMonitor.class, "unitTestRefresh", true);
        }

        @Override
        public Thread newThread(@NotNull final Runnable runnable) {
            final Thread thread = super.newThread(runnable);
            final Thread.UncaughtExceptionHandler existing = thread.getUncaughtExceptionHandler();
            thread.setUncaughtExceptionHandler(
                (final Thread errorThread, final Throwable throwable) -> {
                    ensureUnlocked("unit test refresh pool thread exception handler", null);
                    existing.uncaughtException(errorThread, throwable);
                });
            return thread;
        }
    }

    /**
     * Configure the primary LTM thread or one of the auxiliary refresh threads.
     */
    private void configureRefreshThread() {
        SystemicObjectTracker.markThreadSystemic();
        MultiChunkPool.enableDedicatedPoolForThisThread();
        isRefreshThread.set(true);
    }
}
