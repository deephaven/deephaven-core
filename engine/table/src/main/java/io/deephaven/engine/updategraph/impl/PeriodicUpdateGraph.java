/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.updategraph.impl;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.SleepUtil;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.reference.SimpleReference;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.util.pools.MultiChunkPool;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessManager;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.impl.OperationInitializationThreadPool;
import io.deephaven.engine.table.impl.perf.PerformanceEntry;
import io.deephaven.engine.table.impl.perf.UpdatePerformanceTracker;
import io.deephaven.engine.table.impl.util.StepUpdater;
import io.deephaven.engine.updategraph.*;
import io.deephaven.engine.util.reference.CleanupReferenceProcessorInstance;
import io.deephaven.engine.util.systemicmarking.SystemicObjectTracker;
import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.hotspot.JvmIntrospectionContext;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.log.LogEntry;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.TestUseOnly;
import io.deephaven.util.datastructures.SimpleReferenceManager;
import io.deephaven.util.datastructures.linked.IntrusiveDoublyLinkedNode;
import io.deephaven.util.datastructures.linked.IntrusiveDoublyLinkedQueue;
import io.deephaven.util.function.ThrowingRunnable;
import io.deephaven.util.locks.AwareFunctionalLock;
import io.deephaven.util.process.ProcessEnvironment;
import io.deephaven.util.thread.NamingThreadFactory;
import io.deephaven.util.thread.ThreadInitializationFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.WeakReference;
import java.util.*;
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
public class PeriodicUpdateGraph implements UpdateGraph {

    public static final String DEFAULT_UPDATE_GRAPH_NAME = "DEFAULT";
    public static final int NUM_THREADS_DEFAULT_UPDATE_GRAPH =
            Configuration.getInstance().getIntegerWithDefault("PeriodicUpdateGraph.updateThreads", -1);

    public static Builder newBuilder(final String name) {
        return new Builder(name);
    }

    /**
     * If the provided update graph is a {@link PeriodicUpdateGraph} then create a PerformanceEntry using the given
     * description. Otherwise, return null.
     *
     * @param updateGraph The update graph to create a performance entry for.
     * @param description The description for the performance entry.
     * @return The performance entry, or null if the update graph is not a {@link PeriodicUpdateGraph}.
     */
    @Nullable
    public static PerformanceEntry createUpdatePerformanceEntry(
            final UpdateGraph updateGraph,
            final String description) {
        if (updateGraph instanceof PeriodicUpdateGraph) {
            final PeriodicUpdateGraph pug = (PeriodicUpdateGraph) updateGraph;
            if (pug.updatePerformanceTracker != null) {
                return pug.updatePerformanceTracker.getEntry(description);
            }
            throw new IllegalStateException("Cannot create a performance entry for a PeriodicUpdateGraph that has "
                    + "not been completely constructed.");
        }
        return null;
    }

    private static final KeyedObjectHashMap<String, PeriodicUpdateGraph> INSTANCES = new KeyedObjectHashMap<>(
            new KeyedObjectKey.BasicAdapter<>(PeriodicUpdateGraph::getName));

    private final Logger log = LoggerFactory.getLogger(PeriodicUpdateGraph.class);

    /**
     * Update sources that are part of this PeriodicUpdateGraph.
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
    private volatile boolean running = true;

    /**
     * {@link ScheduledExecutorService} used for scheduling the {@link #watchDogTimeoutProcedure}.
     */
    private final ScheduledExecutorService watchdogScheduler;

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
    public static final String MINIMUM_CYCLE_DURATION_TO_LOG_MILLIS_PROP =
            "PeriodicUpdateGraph.minimumCycleDurationToLogMillis";
    private final long defaultTargetCycleDurationMillis;
    private volatile long targetCycleDurationMillis;
    private final long minimumCycleDurationToLogNanos;
    private final ThreadInitializationFactory threadInitializationFactory;
    private final OperationInitializer threadPool;

    /** when to next flush the performance tracker; initializes to zero to force a flush on start */
    private long nextUpdatePerformanceTrackerFlushTimeNanos;

    /**
     * How many cycles we have not logged, but were non-zero.
     */
    private long suppressedCycles;
    private long suppressedCyclesTotalNanos;
    private long suppressedCyclesTotalSafePointTimeMillis;

    /**
     * Accumulated UpdateGraph exclusive lock waits for the current cycle (or previous, if idle).
     */
    private long currentCycleLockWaitTotalNanos;
    /**
     * Accumulated delays due to intracycle yields for the current cycle (or previous, if idle).
     */
    private long currentCycleYieldTotalNanos;
    /**
     * Accumulated delays due to intracycle sleeps for the current cycle (or previous, if idle).
     */
    private long currentCycleSleepTotalNanos;

    public static class AccumulatedCycleStats {
        /**
         * Number of cycles run.
         */
        public int cycles = 0;
        /**
         * Number of cycles run not exceeding their time budget.
         */
        public int cyclesOnBudget = 0;
        /**
         * Accumulated safepoints over all cycles.
         */
        public int safePoints = 0;
        /**
         * Accumulated safepoint time over all cycles.
         */
        public long safePointPauseTimeMillis = 0L;

        public int[] cycleTimesMicros = new int[32];
        public static final int MAX_DOUBLING_LEN = 1024;

        synchronized void accumulate(
                final long targetCycleDurationMillis,
                final long cycleTimeNanos,
                final long safePoints,
                final long safePointPauseTimeMillis) {
            final boolean onBudget = targetCycleDurationMillis * 1000 * 1000 >= cycleTimeNanos;
            if (onBudget) {
                ++cyclesOnBudget;
            }
            this.safePoints += safePoints;
            this.safePointPauseTimeMillis += safePointPauseTimeMillis;
            if (cycles >= cycleTimesMicros.length) {
                final int newLen;
                if (cycleTimesMicros.length < MAX_DOUBLING_LEN) {
                    newLen = cycleTimesMicros.length * 2;
                } else {
                    newLen = cycleTimesMicros.length + MAX_DOUBLING_LEN;
                }
                cycleTimesMicros = Arrays.copyOf(cycleTimesMicros, newLen);
            }
            cycleTimesMicros[cycles] = (int) ((cycleTimeNanos + 500) / 1_000);
            ++cycles;
        }

        public synchronized void take(final AccumulatedCycleStats out) {
            out.cycles = cycles;
            out.cyclesOnBudget = cyclesOnBudget;
            out.safePoints = safePoints;
            out.safePointPauseTimeMillis = safePointPauseTimeMillis;
            if (out.cycleTimesMicros.length < cycleTimesMicros.length) {
                out.cycleTimesMicros = new int[cycleTimesMicros.length];
            }
            System.arraycopy(cycleTimesMicros, 0, out.cycleTimesMicros, 0, cycles);
            cycles = 0;
            cyclesOnBudget = 0;
            safePoints = 0;
            safePointPauseTimeMillis = 0;
        }
    }

    public final AccumulatedCycleStats accumulatedCycleStats = new AccumulatedCycleStats();

    /**
     * Abstracts away the processing of non-terminal notifications.
     */
    private NotificationProcessor notificationProcessor;

    /**
     * Facilitate GC Introspection during refresh cycles.
     */
    private final JvmIntrospectionContext jvmIntrospectionContext;

    /**
     * The {@link LivenessScope} that should be on top of the {@link LivenessScopeStack} for all run and notification
     * processing. Only non-null while some thread is in {@link #doRefresh(Runnable)}.
     */
    private volatile LivenessScope refreshScope;

    /**
     * The number of threads in our executor service for dispatching notifications. If 1, then we don't actually use the
     * executor service; but instead dispatch all the notifications on the PeriodicUpdateGraph run thread.
     */
    private final int updateThreads;

    /**
     * Is this one of the threads engaged in notification processing? (Either the solitary run thread, or one of the
     * pooled threads it uses in some configurations)
     */
    private final ThreadLocal<Boolean> isUpdateThread = ThreadLocal.withInitial(() -> false);

    private final ThreadLocal<Boolean> serialTableOperationsSafe = ThreadLocal.withInitial(() -> false);

    private final long minimumInterCycleSleep =
            Configuration.getInstance().getIntegerWithDefault("PeriodicUpdateGraph.minimumInterCycleSleep", 0);
    private final boolean interCycleYield =
            Configuration.getInstance().getBooleanWithDefault("PeriodicUpdateGraph.interCycleYield", false);

    private final LogicalClockImpl logicalClock = new LogicalClockImpl();

    /**
     * Encapsulates locking support.
     */
    private final UpdateGraphLock lock;

    /**
     * When PeriodicUpdateGraph.printDependencyInformation is set to true, the PeriodicUpdateGraph will print debug
     * information for each notification that has dependency information; as well as which notifications have been
     * completed and are outstanding.
     */
    private final boolean printDependencyInformation =
            Configuration.getInstance().getBooleanWithDefault("PeriodicUpdateGraph.printDependencyInformation", false);

    private final String name;

    private final UpdatePerformanceTracker updatePerformanceTracker;

    public PeriodicUpdateGraph(
            final String name,
            final boolean allowUnitTestMode,
            final long targetCycleDurationMillis,
            final long minimumCycleDurationToLogNanos,
            final int numUpdateThreads,
            final ThreadInitializationFactory threadInitializationFactory) {
        this.name = name;
        this.allowUnitTestMode = allowUnitTestMode;
        this.defaultTargetCycleDurationMillis = targetCycleDurationMillis;
        this.targetCycleDurationMillis = targetCycleDurationMillis;
        this.minimumCycleDurationToLogNanos = minimumCycleDurationToLogNanos;
        this.threadInitializationFactory = threadInitializationFactory;
        this.threadPool = new OperationInitializationThreadPool(threadInitializationFactory);
        this.lock = UpdateGraphLock.create(this, this.allowUnitTestMode);

        if (numUpdateThreads <= 0) {
            this.updateThreads = Runtime.getRuntime().availableProcessors();
        } else {
            this.updateThreads = numUpdateThreads;
        }

        notificationProcessor = PoisonedNotificationProcessor.INSTANCE;
        jvmIntrospectionContext = new JvmIntrospectionContext();

        refreshThread = new Thread(threadInitializationFactory.createInitializer(() -> {
            configureRefreshThread();
            while (running) {
                Assert.eqFalse(this.allowUnitTestMode, "allowUnitTestMode");
                refreshTablesAndFlushNotifications();
            }
        }), "PeriodicUpdateGraph." + name + ".refreshThread");
        refreshThread.setDaemon(true);
        watchdogScheduler = Executors.newSingleThreadScheduledExecutor(
                new NamingThreadFactory(PeriodicUpdateGraph.class, "watchdogScheduler", true) {
                    @Override
                    public Thread newThread(@NotNull final Runnable r) {
                        // Not a refresh thread, but should still be instrumented for debugging purposes.
                        return super.newThread(threadInitializationFactory.createInitializer(r));
                    }
                });

        updatePerformanceTracker = new UpdatePerformanceTracker(this);
    }

    public String getName() {
        return name;
    }

    public UpdateGraph getUpdateGraph() {
        return this;
    }

    @Override
    public LogOutput append(@NotNull final LogOutput logOutput) {
        return logOutput.append("PeriodicUpdateGraph-").append(name);
    }

    @Override
    public String toString() {
        return new LogOutputStringImpl().append(this).toString();
    }

    @Override
    public LogicalClock clock() {
        return logicalClock;
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

    // region Accessors for the shared and exclusive locks

    /**
     * <p>
     * Get the shared lock for this {@link PeriodicUpdateGraph}.
     * <p>
     * Using this lock will prevent run processing from proceeding concurrently, but will allow other read-only
     * processing to proceed.
     * <p>
     * The shared lock implementation is expected to support reentrance.
     * <p>
     * This lock does <em>not</em> support {@link java.util.concurrent.locks.Lock#newCondition()}. Use the exclusive
     * lock if you need to wait on events that are driven by run processing.
     *
     * @return The shared lock for this {@link PeriodicUpdateGraph}
     */
    public AwareFunctionalLock sharedLock() {
        return lock.sharedLock();
    }

    /**
     * <p>
     * Get the exclusive lock for this {@link PeriodicUpdateGraph}.
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
     * @return The exclusive lock for this {@link PeriodicUpdateGraph}
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
    @Override
    public boolean currentThreadProcessesUpdates() {
        return isUpdateThread.get();
    }

    @Override
    public boolean serialTableOperationsSafe() {
        return serialTableOperationsSafe.get();
    }

    @Override
    public boolean setSerialTableOperationsSafe(final boolean newValue) {
        final boolean old = serialTableOperationsSafe.get();
        serialTableOperationsSafe.set(newValue);
        return old;
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
        if (refreshThread.isAlive()) {
            throw new IllegalStateException("PeriodicUpdateGraph.refreshThread is executing!");
        }
        lock.reset();
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
        synchronized (refreshThread) {
            if (notificationProcessor instanceof PoisonedNotificationProcessor) {
                notificationProcessor = makeNotificationProcessor();
            }
            if (!refreshThread.isAlive()) {
                log.info().append("PeriodicUpdateGraph starting with ").append(updateThreads)
                        .append(" notification processing threads").endl();
                updatePerformanceTracker.start();
                refreshThread.start();
            }
        }
        threadPool.start();
    }

    /**
     * Begins the process to stop all processing threads and forces ReferenceCounted sources to a reference count of
     * zero.
     */
    public void stop() {
        running = false;
        notificationProcessor.shutdown();
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
        if (!running) {
            throw new IllegalStateException("PeriodicUpdateGraph is no longer running");
        }

        if (updateSource instanceof DynamicNode) {
            ((DynamicNode) updateSource).setRefreshing(true);
        }

        if (!allowUnitTestMode) {
            // if we are in unit test mode we never want to start the UpdateGraph
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
                Assert.eq(logicalClock.currentState(), "logicalClock.currentState()",
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
            final long logicalClockValue = logicalClock.currentValue();
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
        StepUpdater.checkForOlderStep(step, sourcesLastSatisfiedStep);
        return sourcesLastSatisfiedStep == step;
    }

    /**
     * Enqueue a collection of notifications to be flushed.
     *
     * @param notifications The notification to enqueue
     *
     * @see #addNotification(Notification)
     */
    @Override
    public void addNotifications(@NotNull final Collection<? extends Notification> notifications) {
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
     * @return Whether this UpdateGraph has a mechanism that supports refreshing
     */
    @Override
    public boolean supportsRefreshing() {
        return true;
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
        isUpdateThread.remove();
        if (randomizedNotifications) {
            notificationProcessor = makeRandomizedNotificationProcessor(notificationRandomizer,
                    maxRandomizedThreadCount, notificationStartDelay);
        } else {
            notificationProcessor = makeNotificationProcessor();
        }
        synchronized (terminalNotifications) {
            terminalNotifications.clear();
        }
        logicalClock.resetForUnitTests();
        sourcesLastSatisfiedStep = logicalClock.currentStep();

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

        lock.reset();
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
        if (sourcesLastSatisfiedStep >= logicalClock.currentStep()) {
            throw new IllegalStateException("Already marked sources as satisfied!");
        }
        sourcesLastSatisfiedStep = logicalClock.currentStep();
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
            Assert.eq(sourcesLastSatisfiedStep, "sourcesLastSatisfiedStep", logicalClock.currentStep(),
                    "logicalClock.currentStep()");
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
            flushNotificationsAndCompleteCycle();
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
        // notifications have been processed. Note that non-update source notifications that require dependency
        // satisfaction are delivered first to the pendingNormalNotifications queue, and hence will not be processed
        // until we advance to the flush* methods.
        // TODO: If and when we properly integrate update sources into the dependency tracking system, we can
        // discontinue this distinct phase, along with the requirement to treat the UpdateGraph itself as a Dependency.
        // Until then, we must delay the beginning of "normal" notification processing until all update sources are
        // done. See IDS-8039.
        notificationProcessor.doAllWork();
        sourcesLastSatisfiedStep = logicalClock.currentStep();

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
                    logicalClock.completeUpdateCycle();
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
                Assert.eqFalse(notification.mustExecuteWithUpdateGraphLock(),
                        "notification.mustExecuteWithUpdateGraphLock()");

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

                if (!notification.mustExecuteWithUpdateGraphLock()) {
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
                    .append(": Exception while executing PeriodicUpdateGraph notification: ").append(notification)
                    .append(": ").append(e).endl();
            ProcessEnvironment.getGlobalFatalErrorReporter()
                    .report("Exception while processing PeriodicUpdateGraph notification", e);
        }
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

    private static final class PoisonedNotificationProcessor implements NotificationProcessor {

        private static final NotificationProcessor INSTANCE = new PoisonedNotificationProcessor();

        private static RuntimeException notYetStarted() {
            return new IllegalStateException("PeriodicUpdateGraph has not been started yet");
        }

        private PoisonedNotificationProcessor() {}

        @Override
        public void submit(@NotNull Notification notification) {
            throw notYetStarted();
        }

        @Override
        public void submitAll(@NotNull IntrusiveDoublyLinkedQueue<Notification> notifications) {
            throw notYetStarted();
        }

        @Override
        public int outstandingNotificationsCount() {
            throw notYetStarted();
        }

        @Override
        public void doWork() {
            throw notYetStarted();
        }

        @Override
        public void doAllWork() {
            throw notYetStarted();
        }

        @Override
        public void shutdown() {}

        @Override
        public void onNotificationAdded() {
            throw notYetStarted();
        }

        @Override
        public void beforeNotificationsDrained() {
            throw notYetStarted();
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

    private static LogEntry appendAsMillisFromNanos(final LogEntry entry, final long nanos) {
        if (nanos > 0) {
            return entry.appendDouble(nanos / 1_000_000.0, 3);
        }
        return entry.append(0);
    }

    /**
     * Iterate over all monitored tables and run them. This method also ensures that the loop runs no faster than
     * {@link #getTargetCycleDurationMillis() minimum cycle time}.
     */
    private void refreshTablesAndFlushNotifications() {
        final long startTimeNanos = System.nanoTime();
        jvmIntrospectionContext.startSample();

        if (sources.isEmpty()) {
            exclusiveLock().doLocked(this::flushTerminalNotifications);
        } else {
            currentCycleLockWaitTotalNanos = currentCycleYieldTotalNanos = currentCycleSleepTotalNanos = 0L;

            ScheduledFuture<?> watchdogFuture = null;

            final long localWatchdogMillis = watchDogMillis;
            final LongConsumer localWatchdogTimeoutProcedure = watchDogTimeoutProcedure;
            if ((localWatchdogMillis > 0) && (localWatchdogTimeoutProcedure != null)) {
                watchdogFuture = watchdogScheduler.schedule(
                        () -> localWatchdogTimeoutProcedure.accept(localWatchdogMillis),
                        localWatchdogMillis, MILLISECONDS);
            }

            refreshAllTables();

            if (watchdogFuture != null) {
                watchdogFuture.cancel(true);
            }
            jvmIntrospectionContext.endSample();
            final long cycleTimeNanos = System.nanoTime() - startTimeNanos;
            computeStatsAndLogCycle(cycleTimeNanos);
        }

        if (interCycleYield) {
            Thread.yield();
        }

        waitForNextCycle(startTimeNanos);
    }

    private void computeStatsAndLogCycle(final long cycleTimeNanos) {
        final long safePointPauseTimeMillis = jvmIntrospectionContext.deltaSafePointPausesTimeMillis();
        accumulatedCycleStats.accumulate(
                getTargetCycleDurationMillis(),
                cycleTimeNanos,
                jvmIntrospectionContext.deltaSafePointPausesCount(),
                safePointPauseTimeMillis);
        if (cycleTimeNanos >= minimumCycleDurationToLogNanos) {
            if (suppressedCycles > 0) {
                logSuppressedCycles();
            }
            final double cycleTimeMillis = cycleTimeNanos / 1_000_000.0;
            LogEntry entry = log.info()
                    .append("Update Graph Processor cycleTime=").appendDouble(cycleTimeMillis, 3);
            if (jvmIntrospectionContext.hasSafePointData()) {
                final long safePointSyncTimeMillis = jvmIntrospectionContext.deltaSafePointSyncTimeMillis();
                entry = entry
                        .append("ms, safePointTime=")
                        .append(safePointPauseTimeMillis)
                        .append("ms, safePointTimePct=");
                if (safePointPauseTimeMillis > 0 && cycleTimeMillis > 0.0) {
                    final double safePointTimePct = 100.0 * safePointPauseTimeMillis / cycleTimeMillis;
                    entry = entry.appendDouble(safePointTimePct, 2);
                } else {
                    entry = entry.append("0");
                }
                entry = entry.append("%, safePointSyncTime=").append(safePointSyncTimeMillis);
            }
            entry = entry.append("ms, lockWaitTime=");
            entry = appendAsMillisFromNanos(entry, currentCycleLockWaitTotalNanos);
            entry = entry.append("ms, yieldTime=");
            entry = appendAsMillisFromNanos(entry, currentCycleSleepTotalNanos);
            entry = entry.append("ms, sleepTime=");
            entry = appendAsMillisFromNanos(entry, currentCycleSleepTotalNanos);
            entry.append("ms").endl();
            return;
        }
        if (cycleTimeNanos > 0) {
            ++suppressedCycles;
            suppressedCyclesTotalNanos += cycleTimeNanos;
            suppressedCyclesTotalSafePointTimeMillis += safePointPauseTimeMillis;
            if (suppressedCyclesTotalNanos >= minimumCycleDurationToLogNanos) {
                logSuppressedCycles();
            }
        }
    }

    private void logSuppressedCycles() {
        LogEntry entry = log.info()
                .append("Minimal Update Graph Processor cycle times: ")
                .appendDouble((double) (suppressedCyclesTotalNanos) / 1_000_000.0, 3).append("ms / ")
                .append(suppressedCycles).append(" cycles = ")
                .appendDouble(
                        (double) suppressedCyclesTotalNanos / (double) suppressedCycles / 1_000_000.0, 3)
                .append("ms/cycle average)");
        if (jvmIntrospectionContext.hasSafePointData()) {
            entry = entry
                    .append(", safePointTime=")
                    .append(suppressedCyclesTotalSafePointTimeMillis)
                    .append("ms");
        }
        entry.endl();
        suppressedCycles = suppressedCyclesTotalNanos = 0;
        suppressedCyclesTotalSafePointTimeMillis = 0;
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
        if (expectedEndTimeNanos >= nextUpdatePerformanceTrackerFlushTimeNanos) {
            nextUpdatePerformanceTrackerFlushTimeNanos =
                    nowNanos + MILLISECONDS.toNanos(UpdatePerformanceTracker.REPORT_INTERVAL_MILLIS);
            try {
                updatePerformanceTracker.flush();
            } catch (Exception err) {
                log.error().append("Error flushing UpdatePerformanceTracker: ").append(err).endl();
            }
        }
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

    /**
     * Refresh all the update sources within an {@link LogicalClock update cycle} after the UpdateGraph has been locked.
     * At the end of the updates all {@link Notification notifications} will be flushed.
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
            final long updatingCycleValue = logicalClock.startUpdateCycle();
            logDependencies().append("Beginning PeriodicUpdateGraph cycle step=")
                    .append(logicalClock.currentStep()).endl();
            try (final SafeCloseable ignored = LivenessScopeStack.open(refreshScope, true)) {
                refreshFunction.run();
                flushNotificationsAndCompleteCycle();
            } finally {
                logicalClock.ensureUpdateCycleCompleted(updatingCycleValue);
                refreshScope = null;
            }
            logDependencies().append("Completed PeriodicUpdateGraph cycle step=")
                    .append(logicalClock.currentStep()).endl();
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

    private class NotificationProcessorThreadFactory extends NamingThreadFactory {
        private NotificationProcessorThreadFactory(@NotNull final ThreadGroup threadGroup, @NotNull final String name) {
            super(threadGroup, PeriodicUpdateGraph.class, name, true);
        }

        @Override
        public Thread newThread(@NotNull final Runnable r) {
            return super.newThread(threadInitializationFactory.createInitializer(() -> {
                configureRefreshThread();
                r.run();
            }));
        }
    }

    @TestUseOnly
    private void ensureUnlocked(@NotNull final String callerDescription, @Nullable final List<String> errors) {
        if (exclusiveLock().isHeldByCurrentThread()) {
            if (errors != null) {
                errors.add(callerDescription + ": UpdateGraph exclusive lock is still held");
            }
            while (exclusiveLock().isHeldByCurrentThread()) {
                exclusiveLock().unlock();
            }
        }
        if (sharedLock().isHeldByCurrentThread()) {
            if (errors != null) {
                errors.add(callerDescription + ": UpdateGraph shared lock is still held");
            }
            while (sharedLock().isHeldByCurrentThread()) {
                sharedLock().unlock();
            }
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
            return super.newThread(() -> {
                configureUnitTestRefreshThread();
                r.run();
            });
        }
    }

    /**
     * Configure the primary UpdateGraph thread or one of the auxiliary notification processing threads.
     */
    private void configureRefreshThread() {
        SystemicObjectTracker.markThreadSystemic();
        MultiChunkPool.enableDedicatedPoolForThisThread();
        isUpdateThread.set(true);
        // Install this UpdateGraph via ExecutionContext for refresh threads
        // noinspection resource
        ExecutionContext.newBuilder().setUpdateGraph(this).build().open();
    }

    /**
     * Configure threads to be used for unit test processing.
     */
    private void configureUnitTestRefreshThread() {
        final Thread currentThread = Thread.currentThread();
        final Thread.UncaughtExceptionHandler existing = currentThread.getUncaughtExceptionHandler();
        currentThread.setUncaughtExceptionHandler((final Thread errorThread, final Throwable throwable) -> {
            ensureUnlocked("unit test run pool thread exception handler", null);
            existing.uncaughtException(errorThread, throwable);
        });
        isUpdateThread.set(true);
        // Install this UpdateGraph via ExecutionContext for refresh threads
        // noinspection resource
        ExecutionContext.newBuilder().setUpdateGraph(this).build().open();
    }

    public void takeAccumulatedCycleStats(AccumulatedCycleStats updateGraphAccumCycleStats) {
        accumulatedCycleStats.take(updateGraphAccumCycleStats);
    }

    public static PeriodicUpdateGraph getInstance(final String name) {
        return INSTANCES.get(name);
    }

    public static final class Builder {
        private final boolean allowUnitTestMode =
                Configuration.getInstance().getBooleanWithDefault(ALLOW_UNIT_TEST_MODE_PROP, false);
        private long targetCycleDurationMillis =
                Configuration.getInstance().getIntegerWithDefault(DEFAULT_TARGET_CYCLE_DURATION_MILLIS_PROP, 1000);
        private long minimumCycleDurationToLogNanos = MILLISECONDS.toNanos(
                Configuration.getInstance().getIntegerWithDefault(MINIMUM_CYCLE_DURATION_TO_LOG_MILLIS_PROP, 25));

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
         *
         * @param threadInitializationFactory
         * @return
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
         * @throws IllegalStateException if a PeriodicUpdateGraph with the provided name already exists
         */
        public PeriodicUpdateGraph build() {
            synchronized (INSTANCES) {
                if (INSTANCES.containsKey(name)) {
                    throw new IllegalStateException(
                            String.format("PeriodicUpdateGraph with name %s already exists", name));
                }
                final PeriodicUpdateGraph newUpdateGraph = construct();
                INSTANCES.put(name, newUpdateGraph);
                return newUpdateGraph;
            }
        }

        /**
         * Returns an existing PeriodicUpdateGraph with the name provided to this Builder, if one exists, else returns a
         * new PeriodicUpdateGraph.
         *
         * @return the PeriodicUpdateGraph
         */
        public PeriodicUpdateGraph existingOrBuild() {
            return INSTANCES.putIfAbsent(name, n -> construct());
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
