package io.deephaven.server.table.stats;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;

/**
 *
 */
public interface ColumnChunkedStatsFunction {
    int CHUNK_SIZE = 2048;

    Table processChunks(final RowSet rowSet, final ColumnSource<?> columnSource, boolean usePrev);
}
