package io.deephaven.engine.updategraph;

import org.jetbrains.annotations.NotNull;

/**
 * Common interface for classes that can register and de-register update sources.
 */
public interface UpdateSourceRegistrar {

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
     * Request that the next update cycle begin as soon as practicable.
     */
    void requestRefresh();
}
