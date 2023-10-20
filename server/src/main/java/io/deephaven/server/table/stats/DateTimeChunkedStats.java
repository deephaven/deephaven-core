package io.deephaven.server.table.stats;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;

public class DateTimeChunkedStats implements ColumnChunkedStatsFunction {
    public Table processChunks(final RowSet index, final ColumnSource<?> columnSource, boolean usePrev) {
        long count = 0;

        long min = QueryConstants.NULL_LONG;
        long max = QueryConstants.NULL_LONG;

        try (final ChunkSource.GetContext getContext =
                columnSource.makeGetContext(ChunkedNumericalStatsKernel.CHUNK_SIZE)) {
            final RowSequence.Iterator okIt = index.getRowSequenceIterator();

            while (okIt.hasMore()) {
                final RowSequence nextKeys = okIt.getNextRowSequenceWithLength(ChunkedNumericalStatsKernel.CHUNK_SIZE);
                final LongChunk<? extends Values> chunk = (usePrev ? columnSource.getPrevChunk(getContext, nextKeys)
                        : columnSource.getChunk(getContext, nextKeys)).asLongChunk();

                final int chunkSize = chunk.size();
                for (int ii = 0; ii < chunkSize; ii++) {
                    final long val = chunk.get(ii);

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
        }

        return TableTools.newTable(
                TableTools.longCol("COUNT", count),
                TableTools.longCol("SIZE", index.size()),
                TableTools.instantCol("MIN", DateTimeUtils.epochNanosToInstant(min)),
                TableTools.instantCol("MAX", DateTimeUtils.epochNanosToInstant(max)));
    }
}
