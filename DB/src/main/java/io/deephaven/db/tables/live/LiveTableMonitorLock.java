package io.deephaven.db.tables.live;

import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.io.logger.Logger;
import io.deephaven.db.tables.utils.QueryPerformanceRecorder;
import io.deephaven.db.v2.sources.LogicalClock;
import io.deephaven.util.FunctionalInterfaces;
import io.deephaven.util.locks.AwareFunctionalLock;
import io.deephaven.internal.log.LoggerFactory;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Lock class to support {@link LiveTableMonitor}.
 */
class LiveTableMonitorLock {

    private static final Logger log = LoggerFactory.getLogger(LiveTableMonitorLock.class);

    private static final boolean STACK_DUMP_LOCKS =
            Configuration.getInstance().getBooleanWithDefault("LiveTableMonitor.stackDumpLocks", false);

    /**
     * The {@link LogicalClock} used for instrumentation and assertions.
     */
    private final LogicalClock logicalClock;

    /**
     * The internal {@link java.util.concurrent.locks.ReadWriteLock} wrapped by this class.
     */
    private final ReentrantReadWriteLock rwLock;

    /**
     * The internal read lock wrapped by the shared lock.
     */
    private final Lock readLock;

    /**
     * The internal write lock wrapped by the exclusive lock.
     */
    private final Lock writeLock;

    /**
     * The shared lock.
     */
    private final AwareFunctionalLock sharedLock;

    /**
     * The exclusive lock.
     */
    private final AwareFunctionalLock exclusiveLock;

    private final boolean allowUnitTestMode;

    /**
     * Construct a lock for a new {@link LiveTableMonitor} instance.
     *
     * @param logicalClock The {@link LogicalClock} instance to use
     */
    LiveTableMonitorLock(@NotNull final LogicalClock logicalClock) {
        this(logicalClock, false);
    }

    /**
     * Construct a lock for a new {@link LiveTableMonitor} instance.
     *
     * @param logicalClock The {@link LogicalClock} instance to use
     * @param allowUnitTestMode for unit tests only
     */
    LiveTableMonitorLock(@NotNull final LogicalClock logicalClock, final boolean allowUnitTestMode) {
        this.logicalClock = logicalClock;
        // TODO: Consider whether using a fair lock causes unacceptable performance degradation under significant
        // contention, and determine an alternative policy (maybe relying on Thread.yield() if so.
        rwLock = new ReentrantReadWriteLock(true);
        readLock = rwLock.readLock();
        writeLock = rwLock.writeLock();
        if (allowUnitTestMode) {
            sharedLock = new DebugAwareFunctionalLock(new SharedLock());
            exclusiveLock = new DebugAwareFunctionalLock(new ExclusiveLock());
        } else {
            sharedLock = new SharedLock();
            exclusiveLock = new ExclusiveLock();
        }
        this.allowUnitTestMode = allowUnitTestMode;
    }

    /**
     * Get the shared lock (similar to {@link java.util.concurrent.locks.ReadWriteLock#readLock()}, but with
     * LTM-specific instrumentation). See {@link LiveTableMonitor#sharedLock()} for user-facing documentation.
     *
     * @return The shared lock
     */
    final AwareFunctionalLock sharedLock() {
        return sharedLock;
    }

    /**
     * Get the exclusive lock (similar to {@link java.util.concurrent.locks.ReadWriteLock#writeLock()} ()}, but with
     * LTM-specific instrumentation). See {@link LiveTableMonitor#exclusiveLock()} for user-facing documentation.
     *
     * @return The exclusive lock
     */
    final AwareFunctionalLock exclusiveLock() {
        return exclusiveLock;
    }

    // region Shared Lock Implementation

    private class SharedLock implements AwareFunctionalLock {

        @Override
        public final boolean isHeldByCurrentThread() {
            return rwLock.getReadHoldCount() > 0;
        }

        @Override
        public final void lock() {
            final MutableBoolean lockSucceeded = new MutableBoolean(false);
            try {
                QueryPerformanceRecorder.withNugget("Acquire LiveTableMonitor readLock", () -> {
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
            final MutableBoolean lockSucceeded = new MutableBoolean(false);
            try {
                QueryPerformanceRecorder.withNuggetThrowing("Acquire LiveTableMonitor readLock interruptibly", () -> {
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
            if (readLock.tryLock()) {
                maybeLogStackTrace("locked (shared)");
                return true;
            }
            return false;
        }

        @Override
        public final boolean tryLock(final long time, @NotNull final TimeUnit unit) throws InterruptedException {
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

    private class ExclusiveLock implements AwareFunctionalLock {

        @Override
        public final boolean isHeldByCurrentThread() {
            return rwLock.isWriteLockedByCurrentThread();
        }

        @Override
        public final void lock() {
            checkForUpgradeAttempt();
            final MutableBoolean lockSucceeded = new MutableBoolean(false);
            try {
                QueryPerformanceRecorder.withNugget("Acquire LiveTableMonitor writeLock", () -> {
                    writeLock.lock();
                    lockSucceeded.setValue(true);
                });
                Assert.eq(logicalClock.currentState(), "logicalClock.currentState()", LogicalClock.State.Idle);
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
            checkForUpgradeAttempt();
            final MutableBoolean lockSucceeded = new MutableBoolean(false);
            try {
                QueryPerformanceRecorder.withNuggetThrowing("Acquire LiveTableMonitor writeLock interruptibly", () -> {
                    writeLock.lockInterruptibly();
                    lockSucceeded.setValue(true);
                });
                Assert.eq(logicalClock.currentState(), "logicalClock.currentState()", LogicalClock.State.Idle);
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
            checkForUpgradeAttempt();
            if (writeLock.tryLock()) {
                maybeLogStackTrace("locked (exclusive)");
                return true;
            }
            return false;
        }

        @Override
        public final boolean tryLock(final long time, @NotNull final TimeUnit unit) throws InterruptedException {
            checkForUpgradeAttempt();
            if (writeLock.tryLock(time, unit)) {
                maybeLogStackTrace("locked (exclusive)");
                return true;
            }
            return false;
        }

        @Override
        public final void unlock() {
            Assert.eq(logicalClock.currentState(), "logicalClock.currentState()", LogicalClock.State.Idle);
            writeLock.unlock();
            maybeLogStackTrace("unlocked (exclusive)");
        }

        @NotNull
        @Override
        public final Condition newCondition() {
            return writeLock.newCondition();
        }
    }

    // endregion Exclusive Lock Implementation

    // region DebugLock
    class DebugAwareFunctionalLock implements AwareFunctionalLock {
        private final AwareFunctionalLock delegate;
        private final Deque<Throwable> lockingContext = new ArrayDeque<>();

        DebugAwareFunctionalLock(AwareFunctionalLock delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean isHeldByCurrentThread() {
            return delegate.isHeldByCurrentThread();
        }

        @Override
        public void lock() {
            delegate.lock();
            lockingContext.push(new Throwable());
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            delegate.lockInterruptibly();
            lockingContext.push(new Throwable());
        }

        @Override
        public boolean tryLock() {
            if (delegate.tryLock()) {
                lockingContext.push(new Throwable());
                return true;
            }
            return false;
        }

        @Override
        public boolean tryLock(long time, @NotNull TimeUnit unit) throws InterruptedException {
            if (delegate.tryLock(time, unit)) {
                lockingContext.push(new Throwable());
                return true;
            }
            return false;
        }

        @Override
        public void unlock() {
            delegate.unlock();
            lockingContext.pop();
        }

        @NotNull
        @Override
        public Condition newCondition() {
            return delegate.newCondition();
        }

        @Override
        public <EXCEPTION_TYPE extends Exception> void doLocked(
                @NotNull FunctionalInterfaces.ThrowingRunnable<EXCEPTION_TYPE> runnable) throws EXCEPTION_TYPE {
            delegate.doLocked(runnable);
        }

        @Override
        public <EXCEPTION_TYPE extends Exception> void doLockedInterruptibly(
                @NotNull FunctionalInterfaces.ThrowingRunnable<EXCEPTION_TYPE> runnable)
                throws InterruptedException, EXCEPTION_TYPE {
            delegate.doLockedInterruptibly(runnable);
        }

        @Override
        public <RESULT_TYPE, EXCEPTION_TYPE extends Exception> RESULT_TYPE computeLocked(
                @NotNull FunctionalInterfaces.ThrowingSupplier<RESULT_TYPE, EXCEPTION_TYPE> supplier)
                throws EXCEPTION_TYPE {
            return delegate.computeLocked(supplier);
        }

        @Override
        public <RESULT_TYPE, EXCEPTION_TYPE extends Exception> RESULT_TYPE computeLockedInterruptibly(
                @NotNull FunctionalInterfaces.ThrowingSupplier<RESULT_TYPE, EXCEPTION_TYPE> supplier)
                throws InterruptedException, EXCEPTION_TYPE {
            return delegate.computeLockedInterruptibly(supplier);
        }

        String getDebugMessage() {
            final Throwable item = lockingContext.peek();
            return item == null ? "locking context is empty" : ExceptionUtils.getStackTrace(item);
        }
    }
    // endregion DebugLock

    // region Validation Methods

    private void checkForUpgradeAttempt() {
        if (sharedLock.isHeldByCurrentThread()) {
            throw new UnsupportedOperationException("Cannot upgrade a shared lock to an exclusive lock");
        }
    }

    // endregion Validation Methods

    // region Debugging Tools

    private static final class LockDebugException extends Exception {

        private LockDebugException(@NotNull final String message) {
            super(message);
        }
    }

    private void maybeLogStackTrace(final String type) {
        if (STACK_DUMP_LOCKS) {
            log.info().append("Live Table Monitor ").append(new LockDebugException(type)).endl();
        }
    }

    // endregion Debugging Tools
}
