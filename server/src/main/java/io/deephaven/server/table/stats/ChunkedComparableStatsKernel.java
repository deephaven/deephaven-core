package io.deephaven.server.table.stats;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import org.apache.commons.text.StringEscapeUtils;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.Collections;
import java.util.Map;

public interface ChunkedComparableStatsKernel<T> {
    int CHUNK_SIZE = ChunkedNumericalStatsKernel.CHUNK_SIZE;

    static ChunkedComparableStatsKernel<?> makeChunkedComparableStatsFactory(final Class<?> type) {
        if (type == Character.class || type == char.class) {
            return new CharacterChunkedComparableStats();
        } else {
            return new ObjectChunkedComparableStats();
        }
    }

    static Table getChunkedComparableStats(final int maxUnique, final Table table, final String columnName,
            boolean usePrev) {
        final ColumnSource<?> columnSource = table.getColumnSource(columnName);
        final RowSet index = usePrev ? table.getRowSet().prev() : table.getRowSet();

        return makeChunkedComparableStatsFactory(columnSource.getType())
                .processChunks(index, columnSource, usePrev, maxUnique);
    }

    Table processChunks(final RowSet index, final ColumnSource<?> columnSource, boolean usePrev, int maxUnique);
}
