package io.deephaven.db.v2.utils;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.db.v2.utils.rsp.RspArray;
import io.deephaven.util.annotations.VisibleForTesting;

/**
 * <p>
 * This class enables a pattern of use where objects can be shared from multiple references/identities while they are
 * used read-only.
 *
 * Note this is not thread safe.
 *
 * A class derives from this class and users of it call getWriteRef() to obtain a reference that can be modified. That
 * may return the same object (with an increased ref count), or may return a deep copy of it if other readers of the
 * object exist. Effectively this creates a copy-on-write sharing strategy.
 * </p>
 *
 * <p>
 * Example:
 * </p>
 *
 * <pre>
 * {@Code
 *
 *   class MyType extends RefCountedCow<MyType> {
 *       &#64;Override protected MyType self() { return this; }
 *       &#64;Override protected MyType copy() { ... } // return a deep copy of this object
 *
 *       ...
 *   }
 *
 *   public class UsesMyType {
 *       private MyType m;
 *
 *       public void acquire() { m.acquire(); }
 *       public void release() { m.release(); }
 *
 *       private void writeCheck() { m = m.getWriteRef(); }
 *
 *       public void someWriteOperation() {
 *           writeCheck();
 *           // regular implementation calling mutation operations on m.
 *       }
 *
 *       public void someReadOnlyOperation() {
 *           // No need to call writeCheck here.
 *       }
 *
 *       ...
 * }
 *
 * }
 * </pre>
 *
 * <p>
 * Uses the "Curiously Recurring Generic Pattern"
 * </p>
 *
 * <p>
 * Note this implementation does minimal concurrency protection, since it assumes it will run under the protection
 * mechanisms of live update table and its clock, ie, reads can concurrently access objects being mutated, but will
 * realize near the end their operation was invalidated by a clock change and will toss their results.
 * </p>
 *
 * @param <T> A class that will extend us, to get RefCounted functionality.
 */
public abstract class RefCountedCow<T> {
    private static final boolean debug = RspArray.debug ||
            Configuration.getInstance().getBooleanForClassWithDefault(
                    RefCountedCow.class, "debug", false);

    /**
     * Field updater for refCount, so we can avoid creating an {@link java.util.concurrent.atomic.AtomicInteger} for
     * each instance.
     */
    private static final AtomicIntegerFieldUpdater<RefCountedCow> REFCOUNT_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(RefCountedCow.class, "refCount");

    /**
     * The actual value of our reference count.
     */
    private volatile int refCount;

    // Helpful sometimes to enable for debugging.
    //
    // private static final AtomicInteger idBase = new AtomicInteger(0);
    // private final int id;

    public RefCountedCow() {
        refCount = 1;
        // Helpful sometimes to enable for debugging.
        // id = idBase.getAndIncrement();
    }

    private int getAndIncrementRefCount() {
        return REFCOUNT_UPDATER.getAndIncrement(this);
    }

    private int decrementAndGetRefCount() {
        return REFCOUNT_UPDATER.decrementAndGet(this);
    }

    private int getRefCount() {
        return refCount;
    }

    /**
     * Increase our reference count.
     */
    public final void acquire() {
        notifyBeforeAcquire();
        final int prev = getAndIncrementRefCount();
        if (debug) {
            Assert.geqZero(prev, "prev");
        }
    }

    /**
     * Obtain a new reference to this object; the reference count will increase. This operation is cheap and does not do
     * a deep copy of the object's payload; if a mutator is called through this reference, the reference will make a
     * copy of its payload first, before applying the mutation, to keep other read only accessor of the previously
     * shared payload unnaffected.
     *
     * Note this assumes a pattern of use for derived classes where mutators return a reference, which may or may not
     * point to the same object on which the mutation was called.
     *
     * Also note this is not thread safe.
     *
     * @return A reference that will copy-on-write on its payload if it is mutated.
     */
    public final T cowRef() {
        acquire();
        return self();
    }

    /**
     * Decrease our reference count.
     *
     * @return the resulting reference count value.
     */
    public final int release() {
        final int count = decrementAndGetRefCount();
        if (debug) {
            Assert.geqZero(count, "prev");
        }
        notifyAfterRelease();
        return count;
    }

    @VisibleForTesting
    public final int refCount() {
        return getRefCount();
    }

    /**
     * Get a deep copy of the current object, not shared with anybody.
     *
     * Note this is not thread safe.
     *
     * @return A full, deep copy of this object with a reference count of 1 (not shared).
     */
    public abstract T deepCopy();

    /**
     * Derived classes should implement self() by simply "return this" of the right type. This method exists only as an
     * implementation artifact for a type safe implementation of the curiously recurring generic pattern.
     *
     * @return this object, with the right, most derived type.
     */
    protected abstract T self();

    /**
     * Derived classes that want to get notified before acquire can override this method.
     */
    protected void notifyBeforeAcquire() {}

    /**
     * Derived classes that want to get notified after release can override this method.
     */
    protected void notifyAfterRelease() {}

    /**
     * Obtain a reference to this object that can be modified without affecting other references. Note this is not
     * thread safe.
     *
     * @return If this object is shared, a deep copy of this object, otherwise the object itself.
     */
    public T getWriteRef() {
        final int count = this.getRefCount();
        if (debug) {
            Assert.gtZero(count, "count");
        }
        return (count > 1)
                ? deepCopy()
                : self();
    }

    /**
     * Query whether this object will copy itself first before mutations. Note this is not thread safe.
     *
     * @return true if this object is not shared and can be mutated directly
     */
    protected boolean canWrite() {
        return getRefCount() <= 1;
    }
}
