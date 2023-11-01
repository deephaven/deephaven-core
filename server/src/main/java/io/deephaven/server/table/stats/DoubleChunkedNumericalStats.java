/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit FloatChunkedNumericalStats and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.server.table.stats;

import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.QueryConstants;

public class DoubleChunkedNumericalStats implements ChunkedNumericalStatsKernel {
    private long count = 0;

    private double sum = .0;
    private double absSum = .0;
    private double sumOfSquares = .0;

    private double min = QueryConstants.NULL_DOUBLE;
    private double max = QueryConstants.NULL_DOUBLE;
    private double absMin = QueryConstants.NULL_DOUBLE;
    private double absMax = QueryConstants.NULL_DOUBLE;

    @Override
    public Table processChunks(final RowSet rowSet, final ColumnSource<?> columnSource, boolean usePrev) {

        try (final ChunkSource.GetContext getContext = columnSource.makeGetContext(CHUNK_SIZE)) {
            final RowSequence.Iterator rsIt = rowSet.getRowSequenceIterator();

            while (rsIt.hasMore()) {
                final RowSequence nextKeys = rsIt.getNextRowSequenceWithLength(CHUNK_SIZE);
                final DoubleChunk<? extends Values> chunk = (usePrev ? columnSource.getPrevChunk(getContext, nextKeys)
                        : columnSource.getChunk(getContext, nextKeys)).asDoubleChunk();

                /*
                 * we'll use these to get as big as we can before adding into a potentially MUCH larger "total" in an
                 * attempt to reduce cumulative loss-of-precision error brought on by FP math.
                 */
                double chunkedSum = .0;
                double chunkedAbsSum = .0;
                double chunkedSumOfSquares = .0;

                final int chunkSize = chunk.size();
                for (int ii = 0; ii < chunkSize; ii++) {
                    final double val = chunk.get(ii);

                    if (val == QueryConstants.NULL_DOUBLE) {
                        continue;
                    }

                    final double absVal = Math.abs(val);

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
                TableTools.doubleCol("MIN", min),
                TableTools.doubleCol("MAX", max),
                TableTools.doubleCol("MIN_ABS", absMin),
                TableTools.doubleCol("MAX_ABS", absMax),
                TableTools.doubleCol("AVG", avg),
                TableTools.doubleCol("AVG_ABS", avg(count, absSum)),
                TableTools.doubleCol("STD_DEV", stdDev(count, sum, sumOfSquares)));
    }
}
