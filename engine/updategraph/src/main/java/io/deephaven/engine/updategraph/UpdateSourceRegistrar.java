//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.updategraph;

import org.jetbrains.annotations.NotNull;

import java.util.Collection;

/**
 * Common interface for classes that can register and de-register update sources.
 */
public interface UpdateSourceRegistrar extends NotificationQueue.Dependency {

    /**
     * Add a source to this registrar.
     *
     * @param updateSource The table to add
     */
    void addSource(@NotNull Runnable updateSource);

    /**
     * Remove a source from this registrar.
     *
     * @param updateSource The table to remove
     */
    void removeSource(@NotNull Runnable updateSource);

    /**
     * Remove a collection of sources from the list of refreshing sources.
     *
     * @implNote This will <i>not</i> set the sources as {@link DynamicNode#setRefreshing(boolean) non-refreshing}.
     * @param sourcesToRemove The sources to remove from the list of refreshing sources
     */
    default void removeSources(final Collection<Runnable> sourcesToRemove) {
        for (final Runnable source : sourcesToRemove) {
            removeSource(source);
        }
    }

    /**
     * Request that the next update cycle begin as soon as practicable.
     */
    void requestRefresh();
}
