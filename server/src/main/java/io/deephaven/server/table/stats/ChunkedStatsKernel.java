//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.stats;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;

/**
 * A function ready to be run in a snapshot, to collect data about a table column.
 */
public interface ChunkedStatsKernel {
    int CHUNK_SIZE = 2048;

    Table processChunks(RowSet rowSet, ColumnSource<?> columnSource, boolean usePrev);
}
