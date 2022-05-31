package io.deephaven.util.datastructures;

import org.jetbrains.annotations.NotNull;

import java.lang.ref.SoftReference;
import java.util.function.Supplier;

/**
 * {@link Supplier} wrapper that caches the result in a {@link SoftReference}. Only suitable to wrap suppliers that are
 * safely repeatable and don't return {@code null}.
 */
public final class LazyCachingSupplier<T> implements Supplier<T> {

    private final Supplier<T> internalSupplier;

    private volatile SoftReference<T> cachedResultRef;

    /**
     * Construct a {@link Supplier} wrapper.
     *
     * @param internalSupplier The {@link Supplier} to wrap. Must be safely repeatable and must not return {@code null}.
     */
    public LazyCachingSupplier(@NotNull final Supplier<T> internalSupplier) {
        this.internalSupplier = internalSupplier;
    }

    @Override
    public T get() {
        SoftReference<T> currentRef;
        T current;
        if ((currentRef = cachedResultRef) != null && (current = currentRef.get()) != null) {
            return current;
        }
        synchronized (this) {
            if ((currentRef = cachedResultRef) != null && (current = currentRef.get()) != null) {
                return current;
            }
            cachedResultRef = new SoftReference<>(current = internalSupplier.get());
        }
        return current;
    }
}
