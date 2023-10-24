package io.deephaven.engine.updategraph;

import io.deephaven.util.SafeCloseable;
import io.deephaven.util.function.ThrowingSupplier;
import io.deephaven.util.locks.FunctionalLock;
import io.deephaven.util.locks.FunctionalReentrantLock;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.Condition;

public class UpdateGraphAwareCompletableFuture<T> implements Future<T> {

    private final UpdateGraph updateGraph;

    /** This condition is used to signal any threads waiting on the UpdateGraph exclusive lock. */
    private volatile Condition updateGraphCondition;

    private final FunctionalLock lock = new FunctionalReentrantLock();
    private volatile Condition lockCondition;

    private volatile ThrowingSupplier<T, ExecutionException> resultSupplier;
    @SuppressWarnings("rawtypes")
    private static final AtomicReferenceFieldUpdater<UpdateGraphAwareCompletableFuture, ThrowingSupplier> RESULT_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(
                    UpdateGraphAwareCompletableFuture.class, ThrowingSupplier.class, "resultSupplier");

    /** The encoding of the cancelled supplier. */
    private static final ThrowingSupplier<?, ExecutionException> CANCELLATION_SUPPLIER = () -> {
        throw new CancellationException();
    };

    public UpdateGraphAwareCompletableFuture(@NotNull final UpdateGraph updateGraph) {
        this.updateGraph = updateGraph;
    }

    ////////////////
    // Future API //
    ////////////////
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        // noinspection unchecked
        return trySignalCompletion((ThrowingSupplier<T, ExecutionException>) CANCELLATION_SUPPLIER);
    }

    @Override
    public boolean isCancelled() {
        return resultSupplier == CANCELLATION_SUPPLIER;
    }

    @Override
    public boolean isDone() {
        return resultSupplier != null;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        checkSharedLockState();

        if (resultSupplier != null) {
            return resultSupplier.get();
        }
        try {
            return getInternal(0, null);
        } catch (TimeoutException toe) {
            throw new IllegalStateException("Unexpected TimeoutException", toe);
        }
    }

    @Override
    public T get(final long timeout, @NotNull final TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        checkSharedLockState();

        if (resultSupplier != null) {
            return resultSupplier.get();
        }
        if (timeout <= 0) {
            throw new TimeoutException();
        }
        return getInternal(timeout, unit);
    }

    private T getInternal(final long timeout, @Nullable final TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        final boolean holdingUpdateGraphLock = updateGraph.exclusiveLock().isHeldByCurrentThread();
        if (holdingUpdateGraphLock) {
            if (updateGraphCondition == null) {
                updateGraphCondition = updateGraph.exclusiveLock().newCondition();
            }
        } else if (lockCondition == null) {
            try (final SafeCloseable ignored = lock.lockCloseable()) {
                if (lockCondition == null) {
                    lockCondition = lock.newCondition();
                }
            }
        }

        if (holdingUpdateGraphLock) {
            waitForResult(updateGraphCondition, timeout, unit);
        } else {
            try (final SafeCloseable ignored = lock.lockCloseable()) {
                waitForResult(lockCondition, timeout, unit);
            }
        }

        return resultSupplier.get();
    }

    private void checkSharedLockState() {
        if (updateGraph.sharedLock().isHeldByCurrentThread()) {
            throw new UnsupportedOperationException(
                    "Cannot Future.get(...) while holding the " + updateGraph + " shared lock");
        }
    }

    private void waitForResult(final Condition condition, final long timeout, @Nullable final TimeUnit unit)
            throws InterruptedException, TimeoutException {
        if (unit == null) {
            while (resultSupplier == null) {
                condition.await();
            }
            return;
        }

        long nanosLeft = unit.toNanos(timeout);
        while (resultSupplier == null) {
            nanosLeft = condition.awaitNanos(nanosLeft);
            if (nanosLeft <= 0) {
                throw new TimeoutException();
            }
        }
    }

    ////////////////////////////////////////////////////
    // Completion API modeled after CompletableFuture //
    ////////////////////////////////////////////////////

    public boolean complete(T value) {
        return trySignalCompletion(() -> value);
    }

    public boolean completeExceptionally(Throwable ex) {
        Objects.requireNonNull(ex);
        return trySignalCompletion(() -> {
            throw new ExecutionException(ex);
        });
    }

    private boolean trySignalCompletion(@NotNull final ThrowingSupplier<T, ExecutionException> result) {
        if (!RESULT_UPDATER.compareAndSet(this, null, result)) {
            return false;
        }

        final Condition localUpdateGraphCondition = updateGraphCondition;
        if (localUpdateGraphCondition != null) {
            updateGraph.requestSignal(localUpdateGraphCondition);
        }
        final Condition localLockCondition = lockCondition;
        if (localLockCondition != null) {
            lock.doLocked(localLockCondition::signalAll);
        }

        return true;
    }
}
