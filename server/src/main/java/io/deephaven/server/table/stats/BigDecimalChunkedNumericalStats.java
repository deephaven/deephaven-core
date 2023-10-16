package io.deephaven.server.table.stats;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.BigDecimalUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

public class BigDecimalChunkedNumericalStats implements ChunkedNumericalStatsKernel<BigDecimal> {
    private final static int SCALE =
            Configuration.getInstance().getIntegerWithDefault("BigDecimalStdOperator.scale", 10);

    private long count = 0;

    private BigDecimal sum = BigDecimal.ZERO;
    private BigDecimal absSum = BigDecimal.ZERO;
    private BigDecimal sqrdSum = BigDecimal.ZERO;

    private BigDecimal min = null;
    private BigDecimal max = null;
    private BigDecimal absMin = null;
    private BigDecimal absMax = null;

    @Override
    public Table processChunks(final RowSet index, final ColumnSource<?> columnSource, boolean usePrev) {

        try (final ChunkSource.GetContext getContext = columnSource.makeGetContext(CHUNK_SIZE)) {
            final RowSequence.Iterator okIt = index.getRowSequenceIterator();

            while (okIt.hasMore()) {
                final RowSequence nextKeys = okIt.getNextRowSequenceWithLength(CHUNK_SIZE);
                final ObjectChunk<BigDecimal, ? extends Values> chunk =
                        (usePrev ? columnSource.getPrevChunk(getContext, nextKeys)
                                : columnSource.getChunk(getContext, nextKeys)).asObjectChunk();

                final int chunkSize = chunk.size();
                for (int ii = 0; ii < chunkSize; ii++) {
                    final BigDecimal val = chunk.get(ii);

                    if (val == null) {
                        continue;
                    }

                    final BigDecimal absVal = val.abs();

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

        BigDecimal c = BigDecimal.valueOf(count);
        BigDecimal avg = count == 0 ? null : sum.divide(c, SCALE, RoundingMode.HALF_UP);
        BigDecimal absAvg = count == 0 ? null : absSum.divide(c, SCALE, RoundingMode.HALF_UP);
        BigDecimal stdDev = count <= 1 ? null
                : BigDecimalUtils.sqrt((sqrdSum.subtract(avg.pow(2).multiply(c)))
                        .divide(BigDecimal.valueOf(count - 1), SCALE, RoundingMode.HALF_UP), SCALE);

        return TableTools.newTable(
                TableTools.longCol("Count", count),
                TableTools.longCol("Size", index.size()),
                new ColumnHolder<>("Sum", BigDecimal.class, null, false, sum),
                new ColumnHolder<>("AbsSum", BigDecimal.class, null, false, absSum),
                new ColumnHolder<>("SqrdSum", BigDecimal.class, null, false, sqrdSum),
                new ColumnHolder<>("Min", BigDecimal.class, null, false, min),
                new ColumnHolder<>("Max", BigDecimal.class, null, false, max),
                new ColumnHolder<>("AbsMin", BigDecimal.class, null, false, absMin),
                new ColumnHolder<>("AbsMax", BigDecimal.class, null, false, absMax),
                new ColumnHolder<>("Avg", BigDecimal.class, null, false, avg),
                new ColumnHolder<>("AbsAvg", BigDecimal.class, null, false,
                        absAvg),
                new ColumnHolder<>("StdDev", BigDecimal.class, null, false, stdDev));
    }
}
