package io.deephaven.engine.tables.live;

import org.jetbrains.annotations.NotNull;

/**
 * Common interface for classes that can register/deregister LiveTables.
 */
public interface UpdateRootRegistrar {

    /**
     * Add a table to this registrar.
     *
     * @param updateRoot The table to add
     */
    void addTable(@NotNull Runnable updateRoot);

    /**
     * Remove a table from this registrar.
     *
     * @param updateRoot The table to remove
     */
    void removeTable(@NotNull Runnable updateRoot);

    void requestRefresh(@NotNull Runnable table);

    void maybeRefreshTable(@NotNull Runnable table, boolean onlyIfHaveLock);
}
