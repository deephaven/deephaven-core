//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.page;

import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.Context;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link ChunkSource.FillContext} implementation for use by {@link PagingChunkSource} implementations. This is
 * basically a re-usable box around an inner {@link Context context} object, filled with whatever the most recently used
 * {@link Page} chose to store.
 */
public class PagingContextHolder implements ChunkSource.FillContext {

    private final int chunkCapacity;
    private final SharedContext sharedContext;

    private Context innerContext;

    public PagingContextHolder(final int chunkCapacity, @Nullable final SharedContext sharedContext) {
        this.chunkCapacity = chunkCapacity;
        this.sharedContext = sharedContext;
    }

    /**
     * {@inheritDoc}
     * 
     * @implNote This implementation always returns {@code true}, as the {@link PageStore} fill implementation follows
     *           an append pattern over multiple {@link Page pages} when necessary, and all known inner context
     *           implementations trivially support unbounded fill. We thus make this a requirement for future inner
     *           context implementations, either naturally or via a slicing/looping pattern.
     */
    @Override
    public boolean supportsUnboundedFill() {
        return true;
    }

    /**
     * Get the chunk capacity this holder was created with.
     * 
     * @return The chunk capacity
     */
    public int getChunkCapacity() {
        return chunkCapacity;
    }

    /**
     * Get the {@link SharedContext} this holder was created with.
     * 
     * @return The {@link SharedContext}
     */
    public SharedContext getSharedContext() {
        return sharedContext;
    }

    /**
     * Get the inner context value set by {@link #setInnerContext(Context)} and cast it to the templated type.
     * 
     * @return The inner context value
     * @param <T> The desired result type
     */
    public <T extends Context> T getInnerContext() {
        // noinspection unchecked
        return (T) innerContext;
    }

    /**
     * Set the inner context object for use by the current region. The previous inner context will be
     * {@link SafeCloseable#close() closed}.
     *
     * @param newInnerContext The new context object
     */
    public void setInnerContext(@Nullable final Context newInnerContext) {
        if (newInnerContext == innerContext) {
            return;
        }
        try (final SafeCloseable ignoredOldInnerContext = innerContext) {
            innerContext = newInnerContext;
        }
    }

    @FunctionalInterface
    public interface Updater {
        /**
         * Provide a new inner context value based on the current state of this holder.
         *
         * @param chunkCapacity The holder's {@link #getChunkCapacity() chunk capacity}
         * @param sharedContext The holder's {@link #getSharedContext() SharedContext}
         * @param currentInnerContext The holder's {@link #getInnerContext() current inner context}
         * @return The new inner context to be held by this holder
         * @param <T> The result type
         */
        @Nullable
        <T extends Context> T updateInnerContext(
                int chunkCapacity,
                @Nullable final SharedContext sharedContext,
                @Nullable final Context currentInnerContext);
    }

    /**
     * Update the inner context value using the provided updater.
     *
     * @param updater The {@link Updater} to use
     * @return The result of {@code updater}
     * @param <T> The desired result type
     */
    public <T extends Context> T updateInnerContext(@NotNull final Updater updater) {
        final T newInnerContext = updater.updateInnerContext(chunkCapacity, sharedContext, innerContext);
        setInnerContext(newInnerContext);
        return newInnerContext;
    }

    @Override
    public void close() {
        setInnerContext(null);
    }
}
