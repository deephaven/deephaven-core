//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.updategraph;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.MultiException;
import io.deephaven.util.annotations.TestUseOnly;
import io.deephaven.util.datastructures.linked.IntrusiveDoublyLinkedNode;
import io.deephaven.util.datastructures.linked.IntrusiveDoublyLinkedQueue;
import io.deephaven.util.function.ThrowingRunnable;
import io.deephaven.util.locks.AwareFunctionalLock;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Lock class to support {@link UpdateGraph}.
 */
public abstract class UpdateGraphLock {

    private static final Logger log = LoggerFactory.getLogger(UpdateGraphLock.class);

    private static final boolean STACK_DUMP_LOCKS =
            Configuration.getInstance().getBooleanWithDefault("UpdateGraphLock.stackDumpLocks", false);

    /**
     * Instrumentation interface for recording lock events.
     */
    public interface Instrumentation {

        default void recordAction(@NotNull final String description, @NotNull final Runnable action) {
            action.run();
        }

        default void recordActionInterruptibly(
                @NotNull final String description,
                @NotNull final ThrowingRunnable<InterruptedException> action)
                throws InterruptedException {
            action.run();
        }
    }

    /**
     * Install an {@link UpdateGraphLock.Instrumentation instrumentation recorder} for all UpdateGraphLock instances.
     *
     * @param instrumentation The {@link UpdateGraphLock.Instrumentation instrumentation recorder}, or {@code null} to
     *        install a no-op recorder.
     */
    public static void installInstrumentation(@Nullable final Instrumentation instrumentation) {
        UpdateGraphLock.instrumentation = instrumentation == null ? new Instrumentation() {} : instrumentation;
    }

    private static Instrumentation instrumentation = new Instrumentation() {};

    /**
     * Construct a lock for a new {@link UpdateGraph} instance.
     *
     * @param updateGraph The {@link UpdateGraph} instance to use
     * @param allowUnitTestMode Whether this lock instance is to be used for unit tests only
     */
    public static UpdateGraphLock create(@NotNull final UpdateGraph updateGraph, final boolean allowUnitTestMode) {
        return allowUnitTestMode
                ? new ResettableUpdateGraphLock(updateGraph)
                : new FinalUpdateGraphLock(updateGraph);
    }

    /**
     * The {@link UpdateGraph} used for instrumentation and assertions.
     */
    protected final UpdateGraph updateGraph;

    /**
     * Construct a lock for a new {@link UpdateGraph} instance.
     *
     * @param updateGraph The {@link UpdateGraph} instance to use
     */
    UpdateGraphLock(@NotNull final UpdateGraph updateGraph) {
        this.updateGraph = updateGraph;
    }

    /**
     * Get the shared lock (similar to {@link java.util.concurrent.locks.ReadWriteLock#readLock()}, but with
     * UGP-specific instrumentation). See {@link UpdateGraph#sharedLock()} for user-facing documentation.
     *
     * @return The shared lock
     */
    public abstract AwareFunctionalLock sharedLock();

    /**
     * Get the exclusive lock (similar to {@link java.util.concurrent.locks.ReadWriteLock#writeLock()} ()}, but with
     * UGP-specific instrumentation). See {@link UpdateGraph#exclusiveLock()} for user-facing documentation.
     *
     * @return The exclusive lock
     */
    public abstract AwareFunctionalLock exclusiveLock();

    /**
     * Reset this UpdateGraphLock between unit tests to ensure a clean slate.
     */
    @TestUseOnly
    public abstract void reset();

    // region Production Implementation

    private static final class FinalUpdateGraphLock extends UpdateGraphLock {

        /**
         * The shared lock.
         */
        private final AwareFunctionalLock sharedLock;

        /**
         * The exclusive lock.
         */
        private final AwareFunctionalLock exclusiveLock;

        private FinalUpdateGraphLock(@NotNull final UpdateGraph updateGraph) {
            super(updateGraph);
            final ReadWriteLockAccessor lockAccessor = new ReentrantReadWriteLockAccessor();
            this.sharedLock = new SharedLock(updateGraph, lockAccessor);
            this.exclusiveLock = new ExclusiveLock(updateGraph, lockAccessor);
        }

        @Override
        public AwareFunctionalLock sharedLock() {
            return sharedLock;
        }

        @Override
        public AwareFunctionalLock exclusiveLock() {
            return exclusiveLock;
        }

        @Override
        @TestUseOnly
        public void reset() {
            throw new UnsupportedOperationException("This UpdateGraphLock instance is not resettable");
        }
    }

    // endregion Production Implementation

    // region Unit Test Implementation

    private static final class ResettableUpdateGraphLock extends UpdateGraphLock {

        /**
         * The ReadWriteLockAccessor used for the current {@link #sharedLock} and {@link #exclusiveLock} pair.
         */
        private RecordedReadWriteLockAccessor lockAccessor;

        /**
         * The shared lock.
         */
        private volatile AwareFunctionalLock sharedLock;

        /**
         * The exclusive lock.
         */
        private volatile AwareFunctionalLock exclusiveLock;

        private ResettableUpdateGraphLock(@NotNull final UpdateGraph updateGraph) {
            super(updateGraph);
            initialize();
        }

        private synchronized void initialize() {
            lockAccessor = new RecordedReadWriteLockAccessor();
            sharedLock = new SharedLock(updateGraph, lockAccessor);
            exclusiveLock = new ExclusiveLock(updateGraph, lockAccessor);
        }

        @Override
        public AwareFunctionalLock sharedLock() {
            return sharedLock;
        }

        @Override
        public AwareFunctionalLock exclusiveLock() {
            return exclusiveLock;
        }

        @Override
        @TestUseOnly
        public synchronized void reset() {
            final RecordingLock writeLock = lockAccessor.writeLock();
            try {
                if (!lockAccessor.readLockIsHeldByCurrentThread()
                        && !lockAccessor.writeLockIsHeldByCurrentThread()
                        && writeLock.delegate.tryLock()) {
                    writeLock.delegate.unlock();
                } else {
                    final RecordingLock readLock = lockAccessor.readLock();
                    final List<LockDebugException> outstandingAcquisitions = Stream.concat(
                            writeLock.getOutstandingRecordedAcquisitions().stream(),
                            readLock.getOutstandingRecordedAcquisitions().stream())
                            .map(rla -> rla.pendingException)
                            .collect(Collectors.toList());
                    throw new UncheckedDeephavenException("UpdateGraphLock held during reset",
                            MultiException.maybeWrapInMultiException("Multiple outstanding recorded acquisitions",
                                    outstandingAcquisitions));
                }
            } finally {
                initialize();
            }
        }
    }

    // endregion Unit Test Implementation

    // region ReadWriteLockAccessor Interface

    /**
     * Lock accessor interface used in order to abstract away details and allow unit test implementations that recover
     * from locking errors.
     */
    private interface ReadWriteLockAccessor {

        /**
         * Test whether the {@link Thread#currentThread() current thread} holds the {@link #readLock() read lock}.
         *
         * @return Whether the current thread holds this ReadWriteLockAccessor's read lock
         */
        boolean readLockIsHeldByCurrentThread();

        /**
         * Get the read {@link Lock lock}.
         *
         * @return This ReadWriteLockAccessor's read lock; this is guaranteed to always return the same reference over
         *         the life of the accessor
         */
        Lock readLock();

        /**
         * Test whether the {@link Thread#currentThread() current thread} holds the {@link #writeLock() write lock}.
         *
         * @return Whether the current thread holds this ReadWriteLockAccessor's write lock
         */
        boolean writeLockIsHeldByCurrentThread();

        /**
         * Get the write {@link Lock lock}.
         *
         * @return This ReadWriteLockAccessor's write lock; this is guaranteed to always return the same reference over
         *         the life of the accessor
         */
        Lock writeLock();
    }

    // endregion ReadWriteLockAccessor Interface

    // region Shared Lock Implementation

    private static class SharedLock implements AwareFunctionalLock {

        /**
         * Logical clock used for correctness checks.
         */
        private final UpdateGraph updateGraph;

        /**
         * Accessor for the underlying lock implementation.
         */
        private final ReadWriteLockAccessor lockAccessor;

        /**
         * The read lock.
         */
        private final Lock readLock;

        private SharedLock(
                @NotNull final UpdateGraph updateGraph,
                @NotNull final ReadWriteLockAccessor lockAccessor) {
            this.updateGraph = updateGraph;
            this.lockAccessor = lockAccessor;
            this.readLock = lockAccessor.readLock();
        }

        @Override
        public final boolean isHeldByCurrentThread() {
            return lockAccessor.readLockIsHeldByCurrentThread();
        }

        @Override
        public final void lock() {
            checkForIllegalLockFromRefreshThread(updateGraph);
            final MutableBoolean lockSucceeded = new MutableBoolean(false);
            try {
                instrumentation.recordAction("Acquire UpdateGraph readLock", () -> {
                    readLock.lock();
                    lockSucceeded.setValue(true);
                });
                maybeLogStackTrace("locked (shared)");
            } catch (Throwable t) {
                // If the recorder instrumentation causes us to throw an exception after the readLock was successfully
                // acquired, we'd better unlock it on the way out.
                if (lockSucceeded.isTrue()) {
                    readLock.unlock();
                }
                throw t;
            }
        }

        @Override
        public final void lockInterruptibly() throws InterruptedException {
            checkForIllegalLockFromRefreshThread(updateGraph);
            final MutableBoolean lockSucceeded = new MutableBoolean(false);
            try {
                instrumentation.recordActionInterruptibly("Acquire UpdateGraph readLock interruptibly",
                        () -> {
                            readLock.lockInterruptibly();
                            lockSucceeded.setValue(true);
                        });
                maybeLogStackTrace("locked (shared)");
            } catch (Throwable t) {
                // If the recorder instrumentation causes us to throw an exception after the readLock was successfully
                // acquired, we'd better unlock it on the way out.
                if (lockSucceeded.isTrue()) {
                    readLock.unlock();
                }
                throw t;
            }
        }

        @Override
        public final boolean tryLock() {
            checkForIllegalLockFromRefreshThread(updateGraph);
            if (readLock.tryLock()) {
                maybeLogStackTrace("locked (shared)");
                return true;
            }
            return false;
        }

        @Override
        public final boolean tryLock(final long time, @NotNull final TimeUnit unit) throws InterruptedException {
            checkForIllegalLockFromRefreshThread(updateGraph);
            if (readLock.tryLock(time, unit)) {
                maybeLogStackTrace("locked (shared)");
                return true;
            }
            return false;
        }

        @Override
        public final void unlock() {
            readLock.unlock();
            maybeLogStackTrace("unlocked (shared)");
        }

        @NotNull
        @Override
        public final Condition newCondition() {
            throw new UnsupportedOperationException("Shared locks do not support conditions");
        }
    }

    // endregion Shared Lock Implementation

    // region Exclusive Lock Implementation

    private static class ExclusiveLock implements AwareFunctionalLock {

        /**
         * Logical clock used for correctness checks.
         */
        private final UpdateGraph updateGraph;

        /**
         * Accessor for the underlying lock implementation.
         */
        private final ReadWriteLockAccessor lockAccessor;

        /**
         * The write lock.
         */
        private final Lock writeLock;

        private ExclusiveLock(
                @NotNull final UpdateGraph updateGraph,
                @NotNull final ReadWriteLockAccessor lockAccessor) {
            this.updateGraph = updateGraph;
            this.lockAccessor = lockAccessor;
            this.writeLock = lockAccessor.writeLock();
        }

        @Override
        public final boolean isHeldByCurrentThread() {
            return lockAccessor.writeLockIsHeldByCurrentThread();
        }

        @Override
        public final void lock() {
            checkForIllegalLockFromRefreshThread(updateGraph);
            checkForUpgradeAttempt();
            final MutableBoolean lockSucceeded = new MutableBoolean(false);
            try {
                instrumentation.recordAction("Acquire UpdateGraph writeLock", () -> {
                    writeLock.lock();
                    lockSucceeded.setValue(true);
                });
                Assert.eq(updateGraph.clock().currentState(), "logicalClock.currentState()", LogicalClock.State.Idle);
                maybeLogStackTrace("locked (exclusive)");
            } catch (Throwable t) {
                // If the recorder instrumentation causes us to throw an exception after the writeLock was
                // successfully acquired, we'd better unlock it on the way out.
                if (lockSucceeded.isTrue()) {
                    writeLock.unlock();
                }
                throw t;
            }
        }

        @Override
        public final void lockInterruptibly() throws InterruptedException {
            checkForIllegalLockFromRefreshThread(updateGraph);
            checkForUpgradeAttempt();
            final MutableBoolean lockSucceeded = new MutableBoolean(false);
            try {
                instrumentation.recordActionInterruptibly("Acquire UpdateGraph writeLock interruptibly",
                        () -> {
                            writeLock.lockInterruptibly();
                            lockSucceeded.setValue(true);
                        });
                Assert.eq(updateGraph.clock().currentState(), "logicalClock.currentState()", LogicalClock.State.Idle);
                maybeLogStackTrace("locked (exclusive)");
            } catch (Throwable t) {
                // If the recorder instrumentation causes us to throw an exception after the writeLock was
                // successfully acquired, we'd better unlock it on the way out.
                if (lockSucceeded.isTrue()) {
                    writeLock.unlock();
                }
                throw t;
            }
        }

        @Override
        public final boolean tryLock() {
            checkForIllegalLockFromRefreshThread(updateGraph);
            checkForUpgradeAttempt();
            if (writeLock.tryLock()) {
                maybeLogStackTrace("locked (exclusive)");
                return true;
            }
            return false;
        }

        @Override
        public final boolean tryLock(final long time, @NotNull final TimeUnit unit) throws InterruptedException {
            checkForIllegalLockFromRefreshThread(updateGraph);
            checkForUpgradeAttempt();
            if (writeLock.tryLock(time, unit)) {
                maybeLogStackTrace("locked (exclusive)");
                return true;
            }
            return false;
        }

        @Override
        public final void unlock() {
            Assert.eq(updateGraph.clock().currentState(), "logicalClock.currentState()", LogicalClock.State.Idle);
            writeLock.unlock();
            maybeLogStackTrace("unlocked (exclusive)");
        }

        @NotNull
        @Override
        public final Condition newCondition() {
            return writeLock.newCondition();
        }

        private void checkForUpgradeAttempt() {
            if (lockAccessor.readLockIsHeldByCurrentThread()) {
                throw new UnsupportedOperationException("Cannot upgrade a shared lock to an exclusive lock");
            }
        }
    }

    // endregion Exclusive Lock Implementation

    // region Lock Safety Validation Helper

    /**
     * Check for inappropriate locking from an update thread during the updating phase.
     * <p>
     * Under normal conditions we expect only the primary (or singular, in single-threaded update graph processors)
     * refresh thread to acquire either lock, and that that thread always acquires the exclusive lock during the idle
     * phase in order to begin the updating phase.
     * <p>
     * Were a worker refresh thread to attempt to acquire either lock without a timeout during the updating phase, it
     * would block forever or until interrupted. Trying to lock with a timeout wouldn't block forever, but would
     * negatively impact the responsiveness of update graph processing. This behavior would &quot;work&quot; for
     * misbehaving notifications under a single-threaded update graph processor with the current implementation, but
     * would immediately become broken upon adding additional update threads. We prefer to proactively prevent
     * notifications from attempting to do this, rather than leave it for users to debug.
     * <p>
     * Note that the worker update threads (if there are any) are never active during the idle phase unless processing
     * terminal notifications that don't require the exclusive lock. Other terminal notifications are processed by the
     * primary refresh thread under the exclusive lock.
     * <p>
     * Two rules follow from this:
     * <ol>
     * <li>It is always safe for an update thread to acquire either lock during the idle phase, as long as other rules
     * are respected (no inversions, no upgrades, and no attempts to wait for the update graph to do work).</li>
     * <li>It is never safe for an update thread to acquire either lock during the updating phase.</li>
     * </ol>
     * <p>
     * Note that this validation only prevents lock attempts from threads belonging to the same {@link UpdateGraph} as
     * this UpdateGraphLock. It may be suitable to lock an UpdateGraph from a thread belonging to a different
     * UpdateGraph if doing so does not introduce any cycles.
     *
     * @param updateGraph The update graph to check for {@link UpdateGraph#clock()#currentState() current state}
     */
    private static void checkForIllegalLockFromRefreshThread(@NotNull final UpdateGraph updateGraph) {
        if (updateGraph.clock().currentState() == LogicalClock.State.Updating
                && updateGraph.currentThreadProcessesUpdates()) {
            // This exception message assumes the misbehavior is from a notification (e.g. for a user listener), rather
            // than an internal programming error.
            throw new UnsupportedOperationException("Non-terminal notifications must not lock the update graph");
        }
    }

    // endregion Lock Safety Validation Helper

    // region ReadWriteLockAccessor implementations

    private static class ReentrantReadWriteLockAccessor implements ReadWriteLockAccessor {

        /**
         * The {@link ReentrantReadWriteLock} wrapped by this class.
         */
        private final ReentrantReadWriteLock rwLock;

        private ReentrantReadWriteLockAccessor() {
            // In the future, we may need to consider whether using a fair lock causes unacceptable performance
            // degradation under significant contention, and determine an alternative policy (maybe relying on
            // Thread.yield() if so.
            rwLock = new ReentrantReadWriteLock(true);
        }

        @Override
        public final boolean readLockIsHeldByCurrentThread() {
            return rwLock.getReadHoldCount() > 0;
        }

        @Override
        public Lock readLock() {
            return rwLock.readLock();
        }

        @Override
        public final boolean writeLockIsHeldByCurrentThread() {
            return rwLock.isWriteLockedByCurrentThread();
        }

        @Override
        public Lock writeLock() {
            return rwLock.writeLock();
        }
    }

    private static final class RecordedReadWriteLockAccessor extends ReentrantReadWriteLockAccessor {

        private final RecordingLock readLock;
        private final RecordingLock writeLock;

        private RecordedReadWriteLockAccessor() {
            this.readLock = new RecordingLock("readLock", super.readLock());
            this.writeLock = new RecordingLock("writeLock", super.writeLock());
        }

        @Override
        public RecordingLock readLock() {
            return readLock;
        }

        @Override
        public RecordingLock writeLock() {
            return writeLock;
        }
    }

    // endregion ReadWriteLockAccessor implementation

    // region RecordedLockAcquisition and RecordingLock

    private static class RecordedLockAcquisition extends IntrusiveDoublyLinkedNode.Impl<RecordedLockAcquisition> {

        private final LockDebugException pendingException;

        private RecordedLockAcquisition(@NotNull final LockDebugException pendingException) {
            this.pendingException = pendingException;
        }
    }

    private static class RecordingLock implements Lock {

        private final String name;
        private final Lock delegate;

        private final IntrusiveDoublyLinkedQueue<RecordedLockAcquisition> outstandingRecordings =
                new IntrusiveDoublyLinkedQueue<>(
                        IntrusiveDoublyLinkedNode.Impl.Adapter.<RecordedLockAcquisition>getInstance());
        private final ThreadLocal<Deque<RecordedLockAcquisition>> threadRecordings =
                ThreadLocal.withInitial(ArrayDeque::new);

        RecordingLock(@NotNull final String name, @NotNull final Lock delegate) {
            this.name = name;
            this.delegate = delegate;
        }

        @Override
        public void lock() {
            delegate.lock();
            pushRecording(new LockDebugException(String.format("Recorded %s.lock()", name)));
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            delegate.lockInterruptibly();
            pushRecording(new LockDebugException(String.format("Recorded %s.lockInterruptibly()", name)));
        }

        @Override
        public boolean tryLock() {
            if (delegate.tryLock()) {
                pushRecording(new LockDebugException(String.format("Recorded %s.tryLock()", name)));
                return true;
            }
            return false;
        }

        @Override
        public boolean tryLock(long time, @NotNull TimeUnit unit) throws InterruptedException {
            if (delegate.tryLock(time, unit)) {
                pushRecording(new LockDebugException(String.format("Recorded %s.tryLock(%d, %s)", name, time, unit)));
                return true;
            }
            return false;
        }

        @Override
        public void unlock() {
            popRecording();
            delegate.unlock();
        }

        @NotNull
        @Override
        public Condition newCondition() {
            return delegate.newCondition();
        }

        private Collection<RecordedLockAcquisition> getOutstandingRecordedAcquisitions() {
            synchronized (outstandingRecordings) {
                if (outstandingRecordings.isEmpty()) {
                    return List.of();
                } else {
                    return outstandingRecordings.stream().collect(Collectors.toList());
                }
            }
        }

        private void pushRecording(@NotNull final LockDebugException pendingException) {
            // Implementation must have already acquired lock
            final RecordedLockAcquisition recordedLockAcquisition;
            try {
                recordedLockAcquisition = new RecordedLockAcquisition(pendingException);
                threadRecordings.get().push(recordedLockAcquisition);
            } catch (Throwable t) {
                delegate.unlock();
                log.warn().append("Unexpected exception while pushing lock context: ").append(t).endl();
                throw t;
            }
            try {
                synchronized (outstandingRecordings) {
                    outstandingRecordings.offer(recordedLockAcquisition);
                }
            } catch (Throwable t) {
                delegate.unlock();
                threadRecordings.get().pop();
                log.warn().append("Unexpected exception while recording outstanding lock context: ").append(t).endl();
                throw t;
            }
        }

        private void popRecording() {
            try {
                final RecordedLockAcquisition recordedLockAcquisition = threadRecordings.get().pop();
                synchronized (outstandingRecordings) {
                    outstandingRecordings.remove(recordedLockAcquisition);
                }
            } catch (Throwable t) {
                log.warn().append("Unexpected exception while popping lock context: ").append(t).endl();
                // Don't re-throw, instead let the caller unlock
            }
        }
    }

    // endregion DebugLock

    // region Debugging Tools

    private static final class LockDebugException extends Exception {

        private LockDebugException(@NotNull final String message) {
            super(String.format("%s: %s", Thread.currentThread().getName(), message));
        }
    }

    private static void maybeLogStackTrace(final String type) {
        if (STACK_DUMP_LOCKS) {
            log.info().append("Update Graph ").append(new LockDebugException(type)).endl();
        }
    }

    // endregion Debugging Tools
}
