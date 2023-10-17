/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ShortChunkedNumericalStats and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.server.table.stats;

import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.QueryConstants;

public class ByteChunkedNumericalStats implements ChunkedNumericalStatsKernel<Byte> {
    private long count = 0;

    private long sum = 0;
    private boolean useFloatingSum = false;
    private double floatingSum = .0;

    private long absSum = 0;
    private boolean useFloatingAbsSum = false;
    private double floatingAbsSum = .0;

    private long sqrdSum = 0;
    private boolean useFloatingSqrdSum = false;
    private double floatingSqrdSum = .0;

    private byte min = QueryConstants.NULL_BYTE;
    private byte max = QueryConstants.NULL_BYTE;
    private byte absMin = QueryConstants.NULL_BYTE;
    private byte absMax = QueryConstants.NULL_BYTE;

    @Override
    public Table processChunks(final RowSet index, final ColumnSource<?> columnSource, boolean usePrev) {

        try (final ChunkSource.GetContext getContext = columnSource.makeGetContext(CHUNK_SIZE)) {
            final RowSequence.Iterator okIt = index.getRowSequenceIterator();

            while (okIt.hasMore()) {
                final RowSequence nextKeys = okIt.getNextRowSequenceWithLength(CHUNK_SIZE);
                final ByteChunk<? extends Values> chunk = (usePrev ? columnSource.getPrevChunk(getContext, nextKeys)
                        : columnSource.getChunk(getContext, nextKeys)).asByteChunk();

                /*
                 * we'll use these to get as big as we can before adding into a potentially MUCH larger "total" in an
                 * attempt to reduce cumulative loss-of-precision error brought on by floating-point math; - but ONLY if
                 * we've overflowed our non-floating-point (long)
                 */
                double chunkedOverflowSum = .0;
                double chunkedOverflowAbsSum = .0;
                double chunkedOverflowSqrdSum = .0;

                final int chunkSize = chunk.size();
                for (int ii = 0; ii < chunkSize; ii++) {
                    final byte val = chunk.get(ii);

                    if (val == QueryConstants.NULL_BYTE) {
                        continue;
                    }

                    final byte absVal = (byte) Math.abs(val);

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

                    if (!useFloatingSum) {
                        try {
                            sum = Math.addExact(sum, val);
                        } catch (final ArithmeticException ae) {
                            useFloatingSum = true;
                            floatingSum = sum;
                            chunkedOverflowSum = val;
                        }
                    } else {
                        chunkedOverflowSum += val;
                    }

                    if (!useFloatingAbsSum) {
                        try {
                            absSum = Math.addExact(absSum, absVal);
                        } catch (final ArithmeticException ae) {
                            useFloatingAbsSum = true;
                            floatingAbsSum = absSum;
                            chunkedOverflowAbsSum = absVal;
                        }
                    } else {
                        chunkedOverflowAbsSum += absVal;
                    }

                    if (!useFloatingSqrdSum) {
                        try {
                            sqrdSum = Math.addExact(sqrdSum, Math.multiplyExact(val, val));
                        } catch (final ArithmeticException ae) {
                            useFloatingSqrdSum = true;
                            floatingSqrdSum = sqrdSum;
                            chunkedOverflowSqrdSum = Math.pow(val, 2);
                        }

                    } else {
                        chunkedOverflowSqrdSum += Math.pow(val, 2);
                    }
                }

                if (useFloatingSum) {
                    floatingSum += chunkedOverflowSum;
                }

                if (useFloatingAbsSum) {
                    floatingAbsSum += chunkedOverflowAbsSum;
                }

                if (useFloatingSqrdSum) {
                    floatingSqrdSum += chunkedOverflowSqrdSum;
                }
            }
        }

        double avg = ChunkedNumericalStatsKernel.avg(count, sum);
        return TableTools.newTable(
                TableTools.longCol("COUNT", count),
                TableTools.longCol("SIZE", index.size()),
                useFloatingSum ? TableTools.doubleCol("SUM", floatingSum) : TableTools.longCol("SUM", sum),
                useFloatingAbsSum ? TableTools.doubleCol("SUM_ABS", floatingAbsSum)
                        : TableTools.longCol("SUM_ABS", absSum),
                useFloatingSqrdSum ? TableTools.doubleCol("SQRD_SUM", floatingSqrdSum)
                        : TableTools.longCol("SUM_SQRD", sqrdSum),
                TableTools.byteCol("MIN", min),
                TableTools.byteCol("MAX", max),
                TableTools.byteCol("MIN_ABS", absMin),
                TableTools.byteCol("MAX_ABS", absMax),
                TableTools.doubleCol("AVG", avg),
                TableTools.doubleCol("AVG_ABS", ChunkedNumericalStatsKernel.avg(count, absSum)),
                TableTools.doubleCol("STD_DEV", ChunkedNumericalStatsKernel.stdDev(count, avg, sqrdSum)));

    }
}
