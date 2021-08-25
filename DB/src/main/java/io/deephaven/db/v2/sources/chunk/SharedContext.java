package io.deephaven.db.v2.sources.chunk;

import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * <p>
 * {@link ResettableContext} used as a holder for other {@link ResettableContext}s that may be
 * shared across components.
 *
 * <p>
 * This serves as a place to cache re-usable computations or resources, but must be {@link #reset()}
 * for every step of an operation (usually a chunk of ordered keys).
 *
 * <p>
 * For example, {@link io.deephaven.db.v2.sources.ReadOnlyRedirectedColumnSource}s that share the
 * same {@link io.deephaven.db.v2.utils.RedirectionIndex} cache a chunk of redirections for the most
 * recent chunk of ordered keys they have been handed.
 *
 * <p>
 * It's important that "nested" usage follows the convention of creating a new instance and passing
 * that instance to context creation methods. Said nested instance should be (or be attached to) an
 * entry in the parent context, and reset/closed when said entry is. It should always be safe to
 * skip nested SharedContext creation if all sources that may be using a given instance will be
 * passed the same ordered keys.
 */
public class SharedContext implements ResettableContext {

    /**
     * The entries in this shared context.
     */
    private final Map<Key, ResettableContext> entries;

    protected SharedContext() {
        entries = new HashMap<>();
    }

    /**
     * Key marker interface.
     *
     * @param <VALUE_TYPE> The type of the context that should be associated with this key type
     */
    @SuppressWarnings("unused")
    // The VALUE_TYPE parameter is in fact used to produce a compile-time association between a key
    // class and its associated value class
    public interface Key<VALUE_TYPE extends ResettableContext> {
    }

    /**
     * Get or create the {@link ResettableContext} value for a {@link Key} key. If the value is
     * computed, the result value will be associated with the {@code key} until the
     * {@link SharedContext} is {@link #close()}ed.
     *
     * @param key The key
     * @param valueFactory The value factory, to be invoked if {@code key} is not found within this
     *        {@link SharedContext}
     * @return The value associated with {@code key}, possibly newly-created
     */
    public final <V extends ResettableContext, K extends Key<V>> V getOrCreate(final K key,
        @NotNull final Supplier<V> valueFactory) {
        // noinspection unchecked
        return (V) entries.computeIfAbsent(key, k -> valueFactory.get());
    }

    /**
     * <p>
     * Reset implementation which invokes {@link ResettableContext#reset()} on all values registered
     * via {@link #getOrCreate(Key, Supplier)}.
     *
     * <p>
     * Sub-classes should be sure to call {@code super.reset()}.
     */
    @Override
    @OverridingMethodsMustInvokeSuper
    public void reset() {
        entries.values().forEach(ResettableContext::reset);
    }

    /**
     * <p>
     * Close implementation which invokes {@link SafeCloseable#close()} on all values registered via
     * {@link #getOrCreate(Key, Supplier)}, and then forgets all registered values.
     *
     * <p>
     * Sub-classes should be sure to call {@code super.close()}.
     */
    @Override
    @OverridingMethodsMustInvokeSuper
    public void close() {
        entries.values().forEach(SafeCloseable::close);
        entries.clear();
    }

    /**
     * Construct a new, empty shared context.
     */
    public static SharedContext makeSharedContext() {
        return new SharedContext();
    }

    /**
     * Abstract {@link Key} implementation for use when a simple Object reference coupled with
     * sub-class identity can determine equality for sharing purposes.
     */
    public static abstract class ExactReferenceSharingKey<VALUE_TYPE extends ResettableContext>
        implements Key<VALUE_TYPE> {

        private final Object differentiator;

        protected ExactReferenceSharingKey(@NotNull final Object differentiator) {
            this.differentiator = differentiator;
        }

        @Override
        public final boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            final ExactReferenceSharingKey otherSharingKey = (ExactReferenceSharingKey) other;
            return differentiator == otherSharingKey.differentiator;
        }

        @Override
        public final int hashCode() {
            return 31
                + 31 * getClass().hashCode()
                + 31 * differentiator.hashCode();
        }
    }
}
