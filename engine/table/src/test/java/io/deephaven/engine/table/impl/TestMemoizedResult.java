//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link MemoizedResult}.
 */
public class TestMemoizedResult {

    private static final long TIMEOUT_SECONDS = 30;

    /**
     * While one caller computes the result, a caller that may not wait computes its own result without waiting, and
     * that result is not cached; the first caller's result is.
     */
    @Test
    public void testCallerThatMayNotWaitComputesWithoutCaching() throws Exception {
        final MemoizedResult<Object> memoizedResult = new MemoizedResult<>();
        final Object firstResult = new Object();
        final Object uncachedResult = new Object();
        final CountDownLatch firstComputing = new CountDownLatch(1);
        final CountDownLatch releaseFirst = new CountDownLatch(1);

        final ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            final Future<Object> firstCall = executor.submit(() -> memoizedResult.getOrCompute(() -> {
                firstComputing.countDown();
                try {
                    assertTrue(releaseFirst.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
                } catch (final InterruptedException e) {
                    throw new AssertionError(e);
                }
                return firstResult;
            }, true));
            try {
                assertTrue(firstComputing.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
                final Future<Object> mayNotWaitCall =
                        executor.submit(() -> memoizedResult.getOrCompute(() -> uncachedResult, false));
                // completes while the first computation is still in progress
                assertSame(uncachedResult, mayNotWaitCall.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
                assertFalse(firstCall.isDone());
            } finally {
                releaseFirst.countDown();
            }
            assertSame(firstResult, firstCall.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertSame(firstResult, memoizedResult.getOrCompute(() -> {
                throw new AssertionError("the cached result should be reused");
            }, false));
        } finally {
            executor.shutdownNow();
        }
    }
}
