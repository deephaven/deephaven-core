package io.deephaven.engine.updategraph;

import junit.framework.TestCase;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.function.Consumer;

/**
 * Unit tests for {@link UpdateGraphLock}.
 */
public class TestUpdateGraphLock {

    @Test
    public void testUpgradeFailures() throws InterruptedException {
        final UpdateGraphLock lock = new UpdateGraphLock(LogicalClock.DEFAULT);

        lock.sharedLock().doLocked(() -> {
            try {
                lock.exclusiveLock().doLocked(() -> TestCase.fail("Unexpectedly upgraded successfully"));
            } catch (UnsupportedOperationException expected) {
            }
        });

        lock.sharedLock().doLockedInterruptibly(() -> {
            try {
                lock.exclusiveLock().doLockedInterruptibly(() -> TestCase.fail("Unexpectedly upgraded successfully"));
            } catch (UnsupportedOperationException expected) {
            }
        });

        TestCase.assertTrue(lock.sharedLock().tryLock());
        try {
            lock.exclusiveLock().tryLock();
            TestCase.fail("Unexpectedly upgraded successfully");
        } catch (UnsupportedOperationException expected) {
        } finally {
            lock.sharedLock().unlock();
        }

        TestCase.assertTrue(lock.sharedLock().tryLock(1, TimeUnit.MILLISECONDS));
        try {
            lock.exclusiveLock().tryLock(1, TimeUnit.MILLISECONDS);
            TestCase.fail("Unexpectedly upgraded successfully");
        } catch (UnsupportedOperationException expected) {
        } finally {
            lock.sharedLock().unlock();
        }
    }

    @Test
    public void testDowngradeSuccess() throws InterruptedException {
        final UpdateGraphLock lock = new UpdateGraphLock(LogicalClock.DEFAULT);

        lock.exclusiveLock().doLocked(() -> {
            final MutableBoolean success = new MutableBoolean(false);
            lock.sharedLock().doLocked(success::setTrue);
            TestCase.assertTrue(success.getValue());
        });

        lock.exclusiveLock().doLockedInterruptibly(() -> {
            final MutableBoolean success = new MutableBoolean(false);
            lock.sharedLock().doLockedInterruptibly(success::setTrue);
            TestCase.assertTrue(success.getValue());
        });

        lock.exclusiveLock().lock();
        lock.sharedLock().lock();
        TestCase.assertTrue(lock.exclusiveLock().isHeldByCurrentThread());
        TestCase.assertTrue(lock.sharedLock().isHeldByCurrentThread());
        lock.exclusiveLock().unlock();
        TestCase.assertFalse(lock.exclusiveLock().isHeldByCurrentThread());
        lock.sharedLock().unlock();
        TestCase.assertFalse(lock.sharedLock().isHeldByCurrentThread());

        lock.exclusiveLock().lockInterruptibly();
        lock.sharedLock().lockInterruptibly();
        TestCase.assertTrue(lock.exclusiveLock().isHeldByCurrentThread());
        TestCase.assertTrue(lock.sharedLock().isHeldByCurrentThread());
        lock.exclusiveLock().unlock();
        TestCase.assertFalse(lock.exclusiveLock().isHeldByCurrentThread());
        lock.sharedLock().unlock();
        TestCase.assertFalse(lock.sharedLock().isHeldByCurrentThread());


        TestCase.assertTrue(lock.exclusiveLock().tryLock());
        TestCase.assertTrue(lock.sharedLock().tryLock());
        TestCase.assertTrue(lock.exclusiveLock().isHeldByCurrentThread());
        TestCase.assertTrue(lock.sharedLock().isHeldByCurrentThread());
        lock.exclusiveLock().unlock();
        TestCase.assertFalse(lock.exclusiveLock().isHeldByCurrentThread());
        lock.sharedLock().unlock();
        TestCase.assertFalse(lock.sharedLock().isHeldByCurrentThread());

        TestCase.assertTrue(lock.exclusiveLock().tryLock(1, TimeUnit.MILLISECONDS));
        TestCase.assertTrue(lock.sharedLock().tryLock(1, TimeUnit.MILLISECONDS));
        TestCase.assertTrue(lock.exclusiveLock().isHeldByCurrentThread());
        TestCase.assertTrue(lock.sharedLock().isHeldByCurrentThread());
        lock.exclusiveLock().unlock();
        TestCase.assertFalse(lock.exclusiveLock().isHeldByCurrentThread());
        lock.sharedLock().unlock();
        TestCase.assertFalse(lock.sharedLock().isHeldByCurrentThread());
    }

    @Test
    public void testSharedLockHeld() {
        final UpdateGraphLock lock = new UpdateGraphLock(LogicalClock.DEFAULT);
        final Consumer<Runnable> checkHeld = (r) -> {
            TestCase.assertTrue(lock.sharedLock().isHeldByCurrentThread());
            lock.sharedLock().doLocked(r::run);
            TestCase.assertTrue(lock.sharedLock().isHeldByCurrentThread());
        };
        final MutableBoolean success = new MutableBoolean(false);
        TestCase.assertFalse(lock.sharedLock().isHeldByCurrentThread());
        lock.sharedLock().doLocked(() -> checkHeld.accept(() -> checkHeld
                .accept(() -> checkHeld.accept(() -> checkHeld.accept(() -> checkHeld.accept(success::setTrue))))));
        TestCase.assertFalse(lock.sharedLock().isHeldByCurrentThread());
        TestCase.assertTrue(success.getValue());
    }

    @Test
    public void testExclusiveLockHeld() {
        final UpdateGraphLock lock = new UpdateGraphLock(LogicalClock.DEFAULT);
        final Consumer<Runnable> checkHeld = (r) -> {
            TestCase.assertTrue(lock.exclusiveLock().isHeldByCurrentThread());
            lock.exclusiveLock().doLocked(r::run);
            TestCase.assertTrue(lock.exclusiveLock().isHeldByCurrentThread());
        };
        final MutableBoolean success = new MutableBoolean(false);
        TestCase.assertFalse(lock.exclusiveLock().isHeldByCurrentThread());
        lock.exclusiveLock().doLocked(() -> checkHeld.accept(() -> checkHeld
                .accept(() -> checkHeld.accept(() -> checkHeld.accept(() -> checkHeld.accept(success::setTrue))))));
        TestCase.assertFalse(lock.exclusiveLock().isHeldByCurrentThread());
    }

    @Test
    public void testConditions() throws InterruptedException {
        final UpdateGraphLock lock = new UpdateGraphLock(LogicalClock.DEFAULT);
        try {
            lock.sharedLock().newCondition();
            TestCase.fail("Unexpectedly got shard lock condition successfully");
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
            TestCase.assertTrue(done.getValue());
        });
    }
}
