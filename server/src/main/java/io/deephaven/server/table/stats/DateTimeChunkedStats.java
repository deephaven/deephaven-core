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

import java.io.Serializable;

public class DateTimeChunkedStats {
    public static Table getStats(final Table table, final ColumnSource<?> columnSource, boolean usePrev) {
        final RowSet index = usePrev ? table.getRowSet().prev() : table.getRowSet();

        return processChunks(index, columnSource, usePrev);
    }

    public static Table processChunks(final RowSet index, final ColumnSource<?> columnSource, boolean usePrev) {
        long count = 0;

        long min = QueryConstants.NULL_LONG;
        long max = QueryConstants.NULL_LONG;

        try (final ChunkSource.GetContext getContext =
                columnSource.makeGetContext(ChunkedNumericalStatsKernel.CHUNK_SIZE)) {
            final RowSequence.Iterator okIt = index.getRowSequenceIterator();

            while (okIt.hasMore()) {
                final RowSequence nextKeys = okIt.getNextRowSequenceWithLength(ChunkedNumericalStatsKernel.CHUNK_SIZE);
                final LongChunk<Values> chunk =
                        (LongChunk<Values>) (usePrev ? columnSource.getPrevChunk(getContext, nextKeys)
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
                TableTools.longCol("Count", count),
                TableTools.longCol("Size", index.size()),
                TableTools.instantCol("Min", DateTimeUtils.epochNanosToInstant(min)),
                TableTools.instantCol("Max", DateTimeUtils.epochNanosToInstant(max)));
    }

    public static class Result implements Serializable {
        private final long size;
        private final long count;

        private final long min;
        private final long max;

        private long runTime = -1;

        public Result(long size, long count, long min, long max) {
            this.size = size;
            this.count = count;

            this.min = min;
            this.max = max;
        }

        private Result setRunTime(final long runTime) {
            this.runTime = runTime;
            return this;
        }

        public long getSize() {
            return size;
        }

        public long getCount() {
            return count;
        }

        public long getMin() {
            return min;
        }

        public long getMax() {
            return max;
        }
    }
}
