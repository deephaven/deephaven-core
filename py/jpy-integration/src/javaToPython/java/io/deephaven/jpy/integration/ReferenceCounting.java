//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.jpy.integration;

import io.deephaven.jpy.GcModule;
import org.jpy.CreateModule;
import org.jpy.PyObject;
import org.junit.Assert;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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
     * This is a fragile method, meant to ensure that GC gets invoked. There are a couple of shortcomings:
     *
     * 1) There is no guarantee that GC will actually be invoked. 2) Even if our dummy object is collected, it doesn't
     * guarantee that any other objects we care about have been GCd.
     *
     * That said - this seems to work for at least some VM implementations.
     */
    public void gc() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        // noinspection unused
        Object obj = new Object() {
            @Override
            protected void finalize() throws Throwable {
                super.finalize();
                latch.countDown();
            }
        };
        // noinspection UnusedAssignment
        obj = null;
        System.gc();
        Assert.assertTrue("GC did not happen within 1 second", latch.await(1, TimeUnit.SECONDS));
        PyObject.cleanup();
        gc.collect();
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
