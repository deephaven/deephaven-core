//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.jpy.integration;

import io.deephaven.jpy.GcModule;
import org.jpy.CreateModule;
import org.jpy.PyObject;
import org.junit.Assert;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

public class ReferenceCounting implements AutoCloseable {

    public static ReferenceCounting create(CreateModule createModule) {
        return new ReferenceCounting(GcModule.create(), RefcountModule.of(createModule));
    }

    private final GcModule gc;
    private final RefcountModule refcountModule;

    private ReferenceCounting(GcModule gc, RefcountModule refcountModule) {
        this.gc = Objects.requireNonNull(gc);
        this.refcountModule = Objects.requireNonNull(refcountModule);
    }

    @Override
    public void close() {
        refcountModule.close();
        gc.close();
    }

    public void check(int expectedReferenceCount, PyObject object) {
        gc.collect();
        Assert.assertEquals(expectedReferenceCount, RefcountModule.refcount(refcountModule, object));
        gc.collect();
        Assert.assertEquals(expectedReferenceCount, RefcountModule.refcount(refcountModule, object));
        blackhole(object);
    }

    public void check(int expectedReferenceCount, Object obj) {
        if (obj instanceof PyObject) {
            check(expectedReferenceCount, (PyObject) obj);
            return;
        }
        final PyObject pyObject = PyObject.unwrapProxy(obj);
        if (pyObject != null) {
            check(expectedReferenceCount, pyObject);
            return;
        }
        throw new IllegalStateException(
                "Should only be checking the python reference count for native PyObjects or proxied PyObjects");
    }

    public int getLogicalRefCount(Object obj) {
        if (obj instanceof PyObject) {
            return getLogicalRefCount((PyObject) obj);
        }
        final PyObject pyObject = PyObject.unwrapProxy(obj);
        if (pyObject != null) {
            return getLogicalRefCount(pyObject);
        }
        throw new IllegalStateException(
                "Should only be getting the python reference count for native PyObjects or proxied PyObjects");
    }

    public int getLogicalRefCount(PyObject obj) {
        return RefcountModule.refcount(refcountModule, obj);
    }

    /**
     * This is a fragile method, whose aspiration is to ensure that garbage collection happens.
     *
     * The strategy is to make three objects: a sentinel object, the caller object under test, and then another sentinel
     * object. The trick here is to create the caller object under test after the first sentinel but before the second
     * sentinel. The underlying assumption is that "surely" the garbage collector won't collect both sentinels without
     * also collecting the caller object.
     *
     * Then we try to induce garbage collection.
     *
     * Then we ask the caller if it looks like their object was finalized.
     *
     * There are three possible outcomes: 1. If the two sentinel objects were not both finalized, return
     * GC_DIDNT_HAPPEN. 2. If the two sentinel objects were finalized but the caller object under test was not, return
     * GC_HAPPENED_BUT_CLIENT_OBJECT_WASNT_FINALIZED. 3. Otherwise, if all three objects were finalized, return SUCCESS.
     *
     * @param makeObject Create the caller object under test and supply it to us. We will set our reference to null at
     *        the appropriate time. The caller should not try to hold a reference to it
     * @param didFinalizationHappen Return true if the caller determines that finalization happened on its object.
     *        Otherwise, return false.
     */
    public <T> CleanupResult doesCleanupHappenAtFinalizerTime(
            Supplier<T> makeObject, BooleanSupplier didFinalizationHappen) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(2);
        Object beforeSentinel = new Object() {
            @Override
            protected void finalize() throws Throwable {
                super.finalize();
                latch.countDown();
            }
        };

        T callerObject = makeObject.get();

        Object afterSentinel = new Object() {
            @Override
            protected void finalize() throws Throwable {
                super.finalize();
                latch.countDown();
            }
        };

        blackhole(beforeSentinel, callerObject, afterSentinel);

        beforeSentinel = null;
        callerObject = null;
        afterSentinel = null;

        // Will try 20 times, for a total of 5 seconds, to collect the garbage
        // and wait for both sentinels to be finalized.
        for (int i = 0; i < 20; i++) {
            System.gc();
            System.runFinalization();

            if (latch.await(250, TimeUnit.MILLISECONDS)) {
                // Both sentinels are finalized. Now see if the caller's
                // state was finalized.
                PyObject.cleanup();
                gc.collect();

                // Wait two more seconds for good luck and to let the above methods
                // settle and do their thing or whatever
                TimeUnit.SECONDS.sleep(2);

                if (didFinalizationHappen.getAsBoolean()) {
                    return CleanupResult.SUCCESS;
                } else {
                    return CleanupResult.GC_HAPPENED_BUT_CLIENT_OBJECT_WASNT_FINALIZED;
                }
            }
        }

        return CleanupResult.GC_DIDNT_HAPPEN;
    }

    public enum CleanupResult {
        SUCCESS, GC_DIDNT_HAPPEN, GC_HAPPENED_BUT_CLIENT_OBJECT_WASNT_FINALIZED
    }

    /**
     * The blackhole ensures that java can't GC away our java objects early (which effects the python reference count)
     */
    public static void blackhole(Object... objects) {
        if (Objects.hash(objects) == Integer.MAX_VALUE) {
            System.out.println("Blackhole"); // very unlikely
        }
    }
}
