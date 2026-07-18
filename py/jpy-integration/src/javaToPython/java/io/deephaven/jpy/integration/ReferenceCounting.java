//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.jpy.integration;

import io.deephaven.jpy.GcModule;
import org.jpy.CreateModule;
import org.jpy.PyObject;
import org.junit.Assert;

import java.lang.ref.Reference;
import java.time.Duration;
import java.util.Objects;
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
        Reference.reachabilityFence(object);
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
     * Rather than trying to demonstrate that cleanup happens at a specific moment (which the JVM does not guarantee,
     * and which made the previous finalizer-sentinel version of this method flaky — see git history), this method only
     * asks that cleanup happen eventually: it repeatedly nudges the JVM garbage collector, drains the jpy reference
     * queue, and runs the Python garbage collector, polling the caller's condition until it becomes true or the timeout
     * expires.
     *
     * @param makeObject Create the caller object under test and supply it to us. We will drop our reference to it
     *        before polling. The caller should not try to hold a reference to it
     * @param didFinalizationHappen Return true if the caller determines that finalization happened on its object.
     *        Otherwise, return false.
     * @return true if cleanup was observed before the timeout, false otherwise
     */
    public <T> boolean doesCleanupHappenEventually(
            Supplier<T> makeObject, BooleanSupplier didFinalizationHappen)
            throws InterruptedException {
        T callerObject = makeObject.get();
        Reference.reachabilityFence(callerObject);
        // noinspection UnusedAssignment
        callerObject = null;

        // Implementation note: the problem with the former test is that it waited for evidence that GC happened, and
        // then *immediately* called PyObject.cleanup().
        // PyObject.cleanup() works by processing swept Weak/PhantomReferences from their queue. There is a race between
        // the garbage collector posting items to that queue and PyObject.cleanup() checking that queue. If
        // PyObject.cleanup() is called too early, it may miss the Weak/PhantomReference
        // being posted. Now, you might think "no worries, if our explicit call to PyObject.cleanup() missed the queue,
        // the dedicated cleanup thread defined in the JPY project at PyObjectReferences.cleanupThreadLogic() will soon
        // run and pick it up; all we need to do is wait a couple of seconds for it to wake up and finish."
        // This was the approach taken by the previous version of the code. The problem is *that thread never runs*
        // because we have disabled it "for now" in py/jpy-integration/src/javaToPython/build.gradle.template.
        // To make this more robust, we repeatedly call all the relevant collection methods in a loop until our PyObject
        // is eventually cleaned up. In ~750 trials the worst case delay was 137 ms, so we are using 5 seconds here,
        // which is a generous margin without stalling a broken run for long.
        final Duration cleanupTimeout = Duration.ofSeconds(5);

        final long deadlineNanos = System.nanoTime() + cleanupTimeout.toNanos();
        while (true) {
            System.gc();
            PyObject.cleanup();
            gc.collect();
            if (didFinalizationHappen.getAsBoolean()) {
                return true;
            }
            if (System.nanoTime() >= deadlineNanos) {
                return false;
            }
            TimeUnit.MILLISECONDS.sleep(100);
        }
    }
}
