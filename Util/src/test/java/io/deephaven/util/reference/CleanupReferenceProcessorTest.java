//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.reference;

import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;

public class CleanupReferenceProcessorTest {

    @Test
    public void registerPhantom() throws InterruptedException, TimeoutException {
        register(CleanupReferenceProcessor.getDefault()::registerPhantom);
    }

    @Test
    public void registerWeak() throws InterruptedException, TimeoutException {
        register(CleanupReferenceProcessor.getDefault()::registerWeak);
    }

    @Ignore("Soft references are harder to test, as they are cleared out at the discretion of the garbage collector based on memory pressure")
    @Test
    public void registerSoft() throws InterruptedException, TimeoutException {
        register(CleanupReferenceProcessor.getDefault()::registerSoft);
    }

    private static void register(BiFunction<Object, Runnable, ?> bf) throws InterruptedException, TimeoutException {
        final CountDownLatch latch = new CountDownLatch(1);
        {
            final Object obj = new Object();
            bf.apply(obj, latch::countDown);
        }
        for (int i = 0; i < 20; ++i) {
            System.gc();
            if (latch.await(100, TimeUnit.MILLISECONDS)) {
                break;
            }
        }
        if (latch.getCount() != 0) {
            throw new TimeoutException();
        }
    }
}
