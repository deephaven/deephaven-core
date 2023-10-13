package io.deephaven.server.table.stats;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.util.BigDecimalUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;

public class BigIntegerChunkedNumericalStats implements ChunkedNumericalStatsKernel<BigInteger> {
    private final static int SCALE = Configuration.getInstance().getIntegerWithDefault("BigDecimalStdOperator.scale", 10);

    private long count = 0;

    private BigInteger sum = BigInteger.ZERO;
    private BigInteger absSum = BigInteger.ZERO;
    private BigInteger sqrdSum = BigInteger.ZERO;

    private BigInteger min = null;
    private BigInteger max = null;
    private BigInteger absMin = null;
    private BigInteger absMax = null;

    @Override
    public Result processChunks(final RowSet index, final ColumnSource<?> columnSource, boolean usePrev) {

        try (final ChunkSource.GetContext getContext = columnSource.makeGetContext(CHUNK_SIZE)) {
            final RowSequence.Iterator okIt = index.getRowSequenceIterator();

            while (okIt.hasMore()) {
                final RowSequence nextKeys = okIt.getNextRowSequenceWithLength(CHUNK_SIZE);
                final ObjectChunk<BigInteger, ? extends Values> chunk = (usePrev ? columnSource.getPrevChunk(getContext, nextKeys) : columnSource.getChunk(getContext, nextKeys)).asObjectChunk();

                final int chunkSize = chunk.size();
                for (int ii = 0; ii < chunkSize; ii++) {
                    final BigInteger val = chunk.get(ii);

                    if (val == null) {
                        continue;
                    }

                    final BigInteger absVal = val.abs();

                    if (count == 0) {
                        min = max = val;
                        absMax = absMin = absVal;
                    } else {
                        if (val.compareTo(min) < 0) {
                            min = val;
                        }

                        if (val.compareTo(max) > 0) {
                            max = val;
                        }

                        if (absVal.compareTo(absMin) < 0) {
                            absMin = absVal;
                        }

                        if (absVal.compareTo(absMax) > 0) {
                            absMax = absVal;
                        }
                    }

                    count++;

                    sum = sum.add(val);
                    absSum = absSum.add(absVal);
                    sqrdSum = sqrdSum.add(absVal.multiply(absVal));
                }
            }
        }

        return new BigIntegerResult(index.size(), count, sum, absSum, sqrdSum, min, max, absMin, absMax);
    }

    private static class BigIntegerResult extends Result implements Serializable {
        private final long count;

        final BigInteger sum;
        final BigInteger absSum;
        final BigInteger sqrdSum;

        BigIntegerResult(long size, long count, final @NotNull BigInteger sum, final @NotNull BigInteger absSum, final @NotNull BigInteger sqrdSum, final @Nullable BigInteger min, final @Nullable BigInteger max, final @Nullable BigInteger absMin, final @Nullable BigInteger absMax) {
            super(size, count, sum, absSum, sqrdSum, min == null ? (Number)Double.NaN : min, max == null ? (Number)Double.NaN : max, absMin == null ? (Number)Double.NaN : absMin, absMax == null ? (Number)Double.NaN : absMax);

            this.count = count;

            this.sum = sum;
            this.absSum = absSum;
            this.sqrdSum = sqrdSum;
        }

        @Override
        public Number getAvg() {
            return count == 0 ? (Number)Double.POSITIVE_INFINITY : getAvg(sum);
        }

        @Override
        public Number getAbsAvg() {
            return count == 0 ? (Number)Double.POSITIVE_INFINITY : getAvg(absSum);
        }

        @Override
        public Number getStdDev() {
            final BigDecimal stdDev = getBigStdDev();
            return stdDev == null ? (Number)Double.NaN : stdDev;
        }

        private BigDecimal getBigStdDev() {
            if (count <= 1) {
                return null;
            }

            final BigDecimal mean = getAvg(sum);
            final BigDecimal var = (new BigDecimal(sqrdSum).subtract(mean.pow(2).multiply(BigDecimal.valueOf(count)))).divide(BigDecimal.valueOf(count-1), SCALE, BigDecimal.ROUND_HALF_UP);
            return BigDecimalUtils.sqrt(var, SCALE);
        }

        private BigDecimal getAvg(final BigInteger val) {
            return new BigDecimal(val).divide(BigDecimal.valueOf(count), SCALE, BigDecimal.ROUND_HALF_UP);
        }
    }
}
