package io.deephaven.server.table.stats;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.util.QueryConstants;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.DecimalFormat;
import java.text.Format;

public interface ChunkedNumericalStatsKernel<T> {
    int CHUNK_SIZE = 2048;

    static ChunkedNumericalStatsKernel<?> makeChunkedNumericalStatsFactory(final Class<?> type) {
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
                    "Invalid type for ChunkedNumericalStatsFactory: " + type.getCanonicalName());
        }
    }

    static Table getChunkedNumericalStats(final Table table, final String columnName, boolean usePrev) {
        final ColumnSource<?> columnSource = table.getColumnSource(columnName);
        final RowSet index = usePrev ? table.getRowSet().prev() : table.getRowSet();

        return ChunkedNumericalStatsKernel.makeChunkedNumericalStatsFactory(columnSource.getType())
                .processChunks(index, columnSource, usePrev);
    }

    Table processChunks(final RowSet index, final ColumnSource<?> columnSource, boolean usePrev);

    static double avg(long count, double sumValue) {
        if (count == 0) {
            return QueryConstants.NULL_DOUBLE;
        }
        return sumValue / count;
    }

    static double stdDev(long count, double avg, double sqrdSum) {
        if (count <= 1) {
            return QueryConstants.NULL_DOUBLE;
        }
        return Math.sqrt((sqrdSum - count * avg * avg) / count - 1);
    }
}
