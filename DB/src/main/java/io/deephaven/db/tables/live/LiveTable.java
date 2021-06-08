/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.live;

/**
 * An object that should be refreshed by the LiveTableMonitor.
 *
 * @apiNote although the interface is called LiveTable, it need not be a Table.
 */
public interface LiveTable {
    /**
     * Refresh this LiveTable.
     */
    void refresh();
}
