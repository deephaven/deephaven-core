/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.tables.live;

/**
 * An object that should be refreshed by the UpdateGraphProcessor.
 *
 * @apiNote although the interface is called LiveTable, it need not be a Table.
 */
public interface LiveTable extends Runnable { // TODO-RWC: Finish deleting live table. Replace with Runnable/updateRoot. isLive->isRefreshing
    /**
     * Refresh this LiveTable.
     */
    void run();
}
