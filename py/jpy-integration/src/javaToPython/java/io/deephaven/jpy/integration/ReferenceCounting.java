package io.deephaven.jpy.integration;

import io.deephaven.jpy.GcModule;
import io.deephaven.jpy.SysModule;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.jpy.PyObject;
import org.junit.Assert;

public class ReferenceCounting implements AutoCloseable {

    public static ReferenceCounting create() {
        return new ReferenceCounting(GcModule.create(), SysModule.create());
    }

    private final GcModule gc;
    private final SysModule sys;

    private ReferenceCounting(GcModule gc, SysModule sys) {
        this.gc = gc;
        this.sys = sys;
    }

    @Override
    public void close() {
        gc.close();
        sys.close();
    }

    public void check(int logicalReferenceCount, Object obj) {
        final PyObject pyObject = PyObject.unwrapProxy(obj);
        if (pyObject != null) {
            obj = pyObject; // todo: arguably, this is something that the the jpy should be doing
                            // itself
        }

        // the extra ref b/c of the tuple ref-stealing (see PyLib_CallAndReturnObject)
        final int extraRefFromTupleStealing = obj instanceof PyObject ? 1 : 0;
        final int extraCount = extraRefFromTupleStealing;
        // this GC might not be technically necessary since python seems to run destructors asap
        // but it shouldn't hurt
        gc.collect();
        Assert.assertEquals(logicalReferenceCount + extraCount, getrefcount(obj));
        // this GC might not be technically necessary since python seems to run destructors asap
        // but it shouldn't hurt
        gc.collect();
        // ensure the getrefcount call didn't change the count
        Assert.assertEquals(logicalReferenceCount + extraCount, getrefcount(obj));
    }

    public int getLogicalRefCount(Object obj) {
        // the extra ref b/c of the tuple ref-stealing (see PyLib_CallAndReturnObject)
        final int extraRefFromTupleStealing = obj instanceof PyObject ? 1 : 0;
        final int extraCount = extraRefFromTupleStealing;
        return getrefcount(obj) - extraCount;
    }

    private int getrefcount(Object o) {
        if (o instanceof PyObject) {
            return sys.getrefcount((PyObject) o);
        }
        return sys.getrefcount(o);
    }

    /**
     * This is a fragile method, meant to ensure that GC gets invoked. There are a couple of
     * shortcomings:
     *
     * 1) There is no guarantee that GC will actually be invoked. 2) Even if our dummy object is
     * collected, it doesn't guarantee that any other objects we care about have been GCd.
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
     * The blackhole ensures that java can't GC away our java objects early (which effects the
     * python reference count)
     */
    public static void blackhole(Object... objects) {
        if (Objects.hash(objects) == Integer.MAX_VALUE) {
            System.out.println("Blackhole"); // very unlikely
        }
    }
}
