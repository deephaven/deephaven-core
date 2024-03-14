//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.stats;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.iterators.ChunkedLongColumnIterator;
import io.deephaven.engine.table.iterators.LongColumnIterator;
import io.deephaven.engine.util.TableTools;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;

public class DateTimeChunkedStats implements ChunkedStatsKernel {
    public Table processChunks(final RowSet rowSet, final ColumnSource<?> columnSource, boolean usePrev) {
        long count = 0;

        long min = QueryConstants.NULL_LONG;
        long max = QueryConstants.NULL_LONG;

        try (LongColumnIterator iterator =
                new ChunkedLongColumnIterator(usePrev ? columnSource.getPrevSource() : columnSource, rowSet)) {
            while (iterator.hasNext()) {
                long val = iterator.nextLong();

                if (val == QueryConstants.NULL_LONG) {
                    continue;
                }

                if (count == 0) {
                    min = max = val;
                } else {
                    if (val < min) {
                        min = val;
                    }

                    if (val > max) {
                        max = val;
                    }
                }

                count++;
            }
        }

        return TableTools.newTable(
                TableTools.longCol("COUNT", count),
                TableTools.longCol("SIZE", rowSet.size()),
                TableTools.instantCol("MIN", DateTimeUtils.epochNanosToInstant(min)),
                TableTools.instantCol("MAX", DateTimeUtils.epochNanosToInstant(max)));
    }
}
