package io.deephaven.db.tables.live;

import org.jetbrains.annotations.NotNull;

/**
 * Common interface for classes that can register/deregister LiveTables.
 */
public interface LiveTableRegistrar {

    /**
     * Add a table to this registrar.
     *
     * @param liveTable The table to add
     */
    void addTable(@NotNull LiveTable liveTable);

    /**
     * Remove a table from this registrar.
     *
     * @param liveTable The table to remove
     */
    void removeTable(@NotNull LiveTable liveTable);

    void requestRefresh(@NotNull LiveTable table);

    void maybeRefreshTable(@NotNull LiveTable table, boolean onlyIfHaveLock);
}
