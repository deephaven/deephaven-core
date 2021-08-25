/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.reference;

/**
 * <p>
 * SimpleReference implementation created to interpose a strong/hard reference in place of a weak reference, with
 * reachability subject to the continued reachability of the wrapped referent via the wrapped reference.
 *
 * <p>
 * In general, this only makes sense for concrete subclasses that are simultaneously T's and SimpleReferences to T's.
 * The intended use case is for callback/listener registration chains that maintain reachability for all but the final
 * link in the chain. Classes that wish to enable this functionality must construct their listener references with
 * maybeCreateWeakReference in order to avoid rendering a WeakReferenceWrapper weakly reachable and thereby breaking the
 * chain.
 */
public abstract class WeakReferenceWrapper<T> implements SimpleReference<T> {

    private final SimpleReference<T> wrappedReference;

    protected WeakReferenceWrapper(T wrappedReferent) {
        this.wrappedReference = maybeCreateWeakReference(wrappedReferent);
    }

    @Override
    public final T get() {
        if (wrappedReference.get() == null) {
            return null;
        }
        // noinspection unchecked
        return (T) this;
    }

    @Override
    public final void clear() {
        wrappedReference.clear();
    }

    protected final T getWrapped() {
        return wrappedReference.get();
    }

    public static <T> SimpleReference<T> maybeCreateWeakReference(T referent) {
        if (referent instanceof WeakReferenceWrapper) {
            // noinspection unchecked
            return (SimpleReference<T>) referent;
        }
        return new WeakSimpleReference<>(referent);
    }
}
