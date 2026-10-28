//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.updategraph;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.function.Consumer;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link UpdateGraphLock}.
 */
public class TestUpdateGraphLock {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    @Test
    public void testUpgradeFailures() throws InterruptedException {
        final UpdateGraphLock lock =
                UpdateGraphLock.create(ExecutionContext.getContext().getUpdateGraph(), false);

        lock.sharedLock().doLocked(() -> {
            try {
                lock.exclusiveLock().doLocked(() -> fail("Unexpectedly upgraded successfully"));
            } catch (UnsupportedOperationException expected) {
            }
        });

        lock.sharedLock().doLockedInterruptibly(() -> {
            try {
                lock.exclusiveLock().doLockedInterruptibly(() -> fail("Unexpectedly upgraded successfully"));
            } catch (UnsupportedOperationException expected) {
            }
        });

        assertTrue(lock.sharedLock().tryLock());
        try {
            lock.exclusiveLock().tryLock();
            fail("Unexpectedly upgraded successfully");
        } catch (UnsupportedOperationException expected) {
        } finally {
            lock.sharedLock().unlock();
        }

        assertTrue(lock.sharedLock().tryLock(1, TimeUnit.MILLISECONDS));
        try {
            lock.exclusiveLock().tryLock(1, TimeUnit.MILLISECONDS);
            fail("Unexpectedly upgraded successfully");
        } catch (UnsupportedOperationException expected) {
        } finally {
            lock.sharedLock().unlock();
        }
    }

    @Test
    public void testDowngradeSuccess() throws InterruptedException {
        final UpdateGraphLock lock =
                UpdateGraphLock.create(ExecutionContext.getContext().getUpdateGraph(), false);

        lock.exclusiveLock().doLocked(() -> {
            final MutableBoolean success = new MutableBoolean(false);
            lock.sharedLock().doLocked(success::setTrue);
            assertTrue(success.getValue());
        });

        lock.exclusiveLock().doLockedInterruptibly(() -> {
            final MutableBoolean success = new MutableBoolean(false);
            lock.sharedLock().doLockedInterruptibly(success::setTrue);
            assertTrue(success.getValue());
        });

        lock.exclusiveLock().lock();
        lock.sharedLock().lock();
        assertTrue(lock.exclusiveLock().isHeldByCurrentThread());
        assertTrue(lock.sharedLock().isHeldByCurrentThread());
        lock.exclusiveLock().unlock();
        assertFalse(lock.exclusiveLock().isHeldByCurrentThread());
        lock.sharedLock().unlock();
        assertFalse(lock.sharedLock().isHeldByCurrentThread());

        lock.exclusiveLock().lockInterruptibly();
        lock.sharedLock().lockInterruptibly();
        assertTrue(lock.exclusiveLock().isHeldByCurrentThread());
        assertTrue(lock.sharedLock().isHeldByCurrentThread());
        lock.exclusiveLock().unlock();
        assertFalse(lock.exclusiveLock().isHeldByCurrentThread());
        lock.sharedLock().unlock();
        assertFalse(lock.sharedLock().isHeldByCurrentThread());


        assertTrue(lock.exclusiveLock().tryLock());
        assertTrue(lock.sharedLock().tryLock());
        assertTrue(lock.exclusiveLock().isHeldByCurrentThread());
        assertTrue(lock.sharedLock().isHeldByCurrentThread());
        lock.exclusiveLock().unlock();
        assertFalse(lock.exclusiveLock().isHeldByCurrentThread());
        lock.sharedLock().unlock();
        assertFalse(lock.sharedLock().isHeldByCurrentThread());

        assertTrue(lock.exclusiveLock().tryLock(1, TimeUnit.MILLISECONDS));
        assertTrue(lock.sharedLock().tryLock(1, TimeUnit.MILLISECONDS));
        assertTrue(lock.exclusiveLock().isHeldByCurrentThread());
        assertTrue(lock.sharedLock().isHeldByCurrentThread());
        lock.exclusiveLock().unlock();
        assertFalse(lock.exclusiveLock().isHeldByCurrentThread());
        lock.sharedLock().unlock();
        assertFalse(lock.sharedLock().isHeldByCurrentThread());
    }

    @Test
    public void testForbiddenWaits() throws Exception {
        final UpdateGraphLock lock =
                UpdateGraphLock.create(ExecutionContext.getContext().getUpdateGraph(), false);

        try (final SafeCloseable ignored = UpdateGraphWaitGuard.forbidWaits()) {
            // available locks are acquired without waiting
            final MutableBoolean success = new MutableBoolean(false);
            lock.sharedLock().doLocked(() -> lock.sharedLock().doLockedInterruptibly(success::setTrue));
            assertTrue(success.getValue());
            success.setFalse();
            lock.exclusiveLock().doLocked(() -> lock.exclusiveLock().doLockedInterruptibly(success::setTrue));
            assertTrue(success.getValue());

            // a pending interrupt is honored, though the lock is available
            Thread.currentThread().interrupt();
            assertThrows(InterruptedException.class, () -> lock.sharedLock().lockInterruptibly());
            assertFalse(Thread.currentThread().isInterrupted());
            assertFalse(lock.sharedLock().isHeldByCurrentThread());
            Thread.currentThread().interrupt();
            assertThrows(InterruptedException.class, () -> lock.exclusiveLock().lockInterruptibly());
            assertFalse(Thread.currentThread().isInterrupted());
            assertFalse(lock.exclusiveLock().isHeldByCurrentThread());
            Thread.currentThread().interrupt();
            assertThrows(InterruptedException.class, () -> lock.sharedLock().tryLock(1, TimeUnit.MINUTES));
            assertFalse(Thread.currentThread().isInterrupted());
            assertFalse(lock.sharedLock().isHeldByCurrentThread());
            Thread.currentThread().interrupt();
            assertThrows(InterruptedException.class, () -> lock.exclusiveLock().tryLock(1, TimeUnit.MINUTES));
            assertFalse(Thread.currentThread().isInterrupted());
            assertFalse(lock.exclusiveLock().isHeldByCurrentThread());
        }
        assertFalse(UpdateGraphWaitGuard.waitsForbidden());

        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            executor.submit(() -> lock.exclusiveLock().lock()).get();
            try (final SafeCloseable ignored = UpdateGraphWaitGuard.forbidWaits()) {
                assertThrows(UpdateGraphWaitForbiddenException.class, () -> lock.sharedLock().lock());
                assertThrows(UpdateGraphWaitForbiddenException.class, () -> lock.sharedLock().lockInterruptibly());
                assertThrows(UpdateGraphWaitForbiddenException.class, () -> lock.exclusiveLock().lock());
                assertThrows(UpdateGraphWaitForbiddenException.class, () -> lock.exclusiveLock().lockInterruptibly());
                assertFalse(lock.sharedLock().tryLock());
                assertFalse(lock.exclusiveLock().tryLock());
                final long beforeTimedAttempts = System.nanoTime();
                assertFalse(lock.sharedLock().tryLock(1, TimeUnit.MINUTES));
                assertFalse(lock.exclusiveLock().tryLock(1, TimeUnit.MINUTES));
                assertTrue(System.nanoTime() - beforeTimedAttempts < TimeUnit.SECONDS.toNanos(30));
                // forbidding waits again, as a nested caller may, leaves waits forbidden when it is done
                try (final SafeCloseable ignored2 = UpdateGraphWaitGuard.forbidWaits()) {
                    assertTrue(UpdateGraphWaitGuard.waitsForbidden());
                }
                assertTrue(UpdateGraphWaitGuard.waitsForbidden());
            }
            assertFalse(UpdateGraphWaitGuard.waitsForbidden());
            assertFalse(lock.sharedLock().isHeldByCurrentThread());
            assertFalse(lock.exclusiveLock().isHeldByCurrentThread());
            executor.submit(() -> lock.exclusiveLock().unlock()).get();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testSharedLockHeld() {
        final UpdateGraphLock lock =
                UpdateGraphLock.create(ExecutionContext.getContext().getUpdateGraph(), false);
        final Consumer<Runnable> checkHeld = (r) -> {
            assertTrue(lock.sharedLock().isHeldByCurrentThread());
            lock.sharedLock().doLocked(r::run);
            assertTrue(lock.sharedLock().isHeldByCurrentThread());
        };
        final MutableBoolean success = new MutableBoolean(false);
        assertFalse(lock.sharedLock().isHeldByCurrentThread());
        lock.sharedLock().doLocked(() -> checkHeld.accept(() -> checkHeld
                .accept(() -> checkHeld.accept(() -> checkHeld.accept(() -> checkHeld.accept(success::setTrue))))));
        assertFalse(lock.sharedLock().isHeldByCurrentThread());
        assertTrue(success.getValue());
    }

    @Test
    public void testExclusiveLockHeld() {
        final UpdateGraphLock lock =
                UpdateGraphLock.create(ExecutionContext.getContext().getUpdateGraph(), false);
        final Consumer<Runnable> checkHeld = (r) -> {
            assertTrue(lock.exclusiveLock().isHeldByCurrentThread());
            lock.exclusiveLock().doLocked(r::run);
            assertTrue(lock.exclusiveLock().isHeldByCurrentThread());
        };
        final MutableBoolean success = new MutableBoolean(false);
        assertFalse(lock.exclusiveLock().isHeldByCurrentThread());
        lock.exclusiveLock().doLocked(() -> checkHeld.accept(() -> checkHeld
                .accept(() -> checkHeld.accept(() -> checkHeld.accept(() -> checkHeld.accept(success::setTrue))))));
        assertFalse(lock.exclusiveLock().isHeldByCurrentThread());
    }

    @Test
    public void testConditions() throws InterruptedException {
        final UpdateGraphLock lock =
                UpdateGraphLock.create(ExecutionContext.getContext().getUpdateGraph(), false);
        try {
            lock.sharedLock().newCondition();
            fail("Unexpectedly got shard lock condition successfully");
        } catch (UnsupportedOperationException expected) {
        }

        final Condition condition = lock.exclusiveLock().newCondition();
        lock.exclusiveLock().doLocked(() -> {
            final MutableBoolean done = new MutableBoolean(false);
            new Thread(() -> {
                lock.exclusiveLock().doLocked(() -> {
                    done.setTrue();
                    condition.signal();
                });
            }).start();
            condition.await(1, TimeUnit.SECONDS);
            // Technically, this is a random-failer, but I expect it to be fine.
            assertTrue(done.getValue());
        });
    }

    @Test
    public void testDebugImplementation() {
        final UpdateGraphLock lock =
                UpdateGraphLock.create(ExecutionContext.getContext().getUpdateGraph(), true);
        lock.sharedLock().lock();
        lock.sharedLock().lock();
        try {
            lock.reset();
            fail("Expected exception");
        } catch (UncheckedDeephavenException expected) {
            expected.printStackTrace();
        }
        lock.exclusiveLock().lock();
        try {
            lock.reset();
            fail("Expected exception");
        } catch (UncheckedDeephavenException expected) {
            expected.printStackTrace();
        }
        lock.reset();
    }
}
