//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.reference;

import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.concurrent.Callable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PooledObjectReferenceTest {
    private final Object obj = new Object();
    private final MyReference ref = new MyReference(obj);

    @Test
    public void clear() {
        assertNotReturned(false);
        ref.clear();
        assertReturned();
    }

    @Test
    public void clearClear() {
        assertNotReturned(false);
        ref.clear();
        ref.clear();
        assertReturned();
    }

    @Test
    public void acquireClear() throws Exception {
        withAcquireIfAvailable(() -> {
            assertNotReturned(false);
            ref.clear();
            assertNotReturned(true);
            return null;
        });
        assertReturned();
    }

    @Test
    public void acquireAcquireClear() throws Exception {
        withAcquireIfAvailable(() -> {
            assertNotReturned(false);
            withAcquireIfAvailable(() -> {
                assertNotReturned(false);
                ref.clear();
                assertNotReturned(true);
                return null;
            });
            assertNotReturned(true);
            return null;
        });
        assertReturned();
    }

    @Test
    public void acquireClearAcquire() throws Exception {
        withAcquireIfAvailable(() -> {
            assertNotReturned(false);
            ref.clear();
            withAcquire(() -> {
                assertNotReturned(true);
                return null;
            });
            assertNotReturned(true);
            return null;
        });
        assertReturned();
    }

    @Test
    public void illegalRelease() {
        try {
            ref.release();
            fail("Expected IllegalStateException");
        } catch (IllegalStateException e) {
            // expected
        }
        assertNoAcquire();
        ref.clear();
        assertReturned();
    }

    @Test
    public void illegalGetClear() {
        // Even though illegal, no errors and eq obj
        assertEquals(obj, ref.get());
        ref.clear();
        assertReturned();
    }

    @Test
    public void clearIllegalGet() {
        ref.clear();
        assertReturned();
        // Even though illegal, no errors and eq null
        assertNull(ref.get());
    }

    @Test
    public void clearWithErrorIllegalGet() {
        ref.throwOnReturnToPool();
        try {
            ref.clear();
            fail("Expected ThrowOnReturnToPool");
        } catch (ThrowOnReturnToPool e) {
            // expected
        }
        // Even though pool had error, we are no longer able to acquire, and get is null
        assertNoAcquire();
        // Even though illegal, no errors and eq null
        assertNull(ref.get());
    }

    void withAcquire(Callable<?> callable) throws Exception {
        assertTrue(ref.acquire());
        try {
            assertEquals(obj, ref.get());
            callable.call();
        } finally {
            ref.release();
        }
    }

    void withAcquireIfAvailable(Callable<?> callable) throws Exception {
        // acquireIfAvailable is stricter than acquire, so acquire should always succeed here
        assertTrue(ref.acquire());
        ref.release();

        assertTrue(ref.acquireIfAvailable());
        try {
            assertEquals(obj, ref.get());
            callable.call();
        } finally {
            ref.release();
        }
    }

    void assertReturned() {
        assertTrue("ref.returned", ref.returned);
        assertFalse(ref.acquire());
        assertFalse(ref.acquireIfAvailable());
        assertNull(ref.acquireAndGet());
        // Even though illegal, no errors and eq null
        assertNull(ref.get());
    }

    void assertNotReturned(boolean isCleared) {
        assertFalse("ref.returned", ref.returned);
        // We know we can acquire
        assertTrue(ref.acquire());
        ref.release();
        if (isCleared) {
            assertFalse(ref.acquireIfAvailable());
        } else {
            assertTrue(ref.acquireIfAvailable());
            ref.release();
        }
    }

    void assertNoAcquire() {
        assertFalse(ref.acquire());
        assertFalse(ref.acquireIfAvailable());
    }

    static final class MyReference extends PooledObjectReference<Object> {
        private boolean returned;
        private boolean throwOnReturnToPool;

        MyReference(@NotNull final Object buffer) {
            super(buffer);
        }

        public void throwOnReturnToPool() {
            throwOnReturnToPool = true;
        }

        @Override
        protected void returnReferentToPool(@NotNull Object referent) {
            if (throwOnReturnToPool) {
                throw new ThrowOnReturnToPool();
            }
            returned = true;
        }
    }

    static final class ThrowOnReturnToPool extends RuntimeException {

    }
}
