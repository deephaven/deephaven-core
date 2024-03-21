//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.stats;

import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.QueryConstants;

public class FloatChunkedNumericalStats implements ChunkedNumericalStatsKernel {
    private long count = 0;

    private double sum = .0;
    private double absSum = .0;
    private double sumOfSquares = .0;

    private float min = QueryConstants.NULL_FLOAT;
    private float max = QueryConstants.NULL_FLOAT;
    private float absMin = QueryConstants.NULL_FLOAT;
    private float absMax = QueryConstants.NULL_FLOAT;

    @Override
    public Table processChunks(final RowSet rowSet, final ColumnSource<?> columnSource, boolean usePrev) {

        try (final ChunkSource.GetContext getContext = columnSource.makeGetContext(CHUNK_SIZE)) {
            final RowSequence.Iterator rsIt = rowSet.getRowSequenceIterator();

            while (rsIt.hasMore()) {
                final RowSequence nextKeys = rsIt.getNextRowSequenceWithLength(CHUNK_SIZE);
                final FloatChunk<? extends Values> chunk = (usePrev ? columnSource.getPrevChunk(getContext, nextKeys)
                        : columnSource.getChunk(getContext, nextKeys)).asFloatChunk();

                /*
                 * we'll use these to get as big as we can before adding into a potentially MUCH larger "total" in an
                 * attempt to reduce cumulative loss-of-precision error brought on by FP math.
                 */
                double chunkedSum = .0;
                double chunkedAbsSum = .0;
                double chunkedSumOfSquares = .0;

                final int chunkSize = chunk.size();
                for (int ii = 0; ii < chunkSize; ii++) {
                    final float val = chunk.get(ii);

                    if (val == QueryConstants.NULL_FLOAT) {
                        continue;
                    }

                    final float absVal = Math.abs(val);

                    if (count == 0) {
                        min = max = val;
                        absMax = absMin = absVal;
                    } else {
                        if (val < min) {
                            min = val;
                        }

                        if (val > max) {
                            max = val;
                        }

                        if (absVal < absMin) {
                            absMin = absVal;
                        }

                        if (absVal > absMax) {
                            absMax = absVal;
                        }
                    }

                    count++;

                    chunkedSum += val;
                    chunkedAbsSum += absVal;
                    chunkedSumOfSquares += (double) val * (double) val;
                }

                sum += chunkedSum;
                absSum += chunkedAbsSum;
                sumOfSquares += chunkedSumOfSquares;
            }
        }

        double avg = avg(count, sum);
        return TableTools.newTable(
                TableTools.longCol("COUNT", count),
                TableTools.longCol("SIZE", rowSet.size()),
                TableTools.doubleCol("SUM", sum),
                TableTools.doubleCol("SUM_ABS", absSum),
                TableTools.doubleCol("SUM_SQRD", sumOfSquares),
                TableTools.floatCol("MIN", min),
                TableTools.floatCol("MAX", max),
                TableTools.floatCol("MIN_ABS", absMin),
                TableTools.floatCol("MAX_ABS", absMax),
                TableTools.doubleCol("AVG", avg),
                TableTools.doubleCol("AVG_ABS", avg(count, absSum)),
                TableTools.doubleCol("STD_DEV", stdDev(count, sum, sumOfSquares)));
    }
}
