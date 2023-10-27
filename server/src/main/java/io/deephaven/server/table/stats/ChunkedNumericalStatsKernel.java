package io.deephaven.server.table.stats;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.util.QueryConstants;

import java.math.BigDecimal;
import java.math.BigInteger;

public interface ChunkedNumericalStatsKernel extends ChunkedStatsKernel {

    static ChunkedNumericalStatsKernel makeChunkedNumericalStatsFactory(final Class<?> type) {
        if (type == Long.class || type == long.class) {
            return new LongChunkedNumericalStats();
        } else if (type == Double.class || type == double.class) {
            return new DoubleChunkedNumericalStats();
        } else if (type == Integer.class || type == int.class) {
            return new IntegerChunkedNumericalStats();
        } else if (type == Float.class || type == float.class) {
            return new FloatChunkedNumericalStats();
        } else if (type == BigInteger.class) {
            return new BigIntegerChunkedNumericalStats();
        } else if (type == BigDecimal.class) {
            return new BigDecimalChunkedNumericalStats();
        } else if (type == Short.class || type == short.class) {
            return new ShortChunkedNumericalStats();
        } else if (type == Byte.class || type == byte.class) {
            return new ByteChunkedNumericalStats();
        } else {
            throw new IllegalStateException(
                    "Invalid type for ChunkedNumericalStatsKernel: " + type.getCanonicalName());
        }
    }

    Table processChunks(final RowSet rowSet, final ColumnSource<?> columnSource, boolean usePrev);

    default double avg(long count, double sumValue) {
        if (count == 0) {
            return QueryConstants.NULL_DOUBLE;
        }
        return sumValue / count;
    }

    default double stdDev(long count, double avg, double sqrdSum) {
        if (count <= 1) {
            return QueryConstants.NULL_DOUBLE;
        }
        return Math.sqrt((sqrdSum - count * avg * avg) / (count - 1));
    }
}
