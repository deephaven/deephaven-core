package io.deephaven.server.table.stats;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;

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
            throw new IllegalStateException("Invalid type for ChunkedNumericalStatsFactory: " + type.getCanonicalName());
        }
    }

    static Result getChunkedNumericalStats(final Table table, final String columnName, boolean usePrev) {
        final ColumnSource<?> columnSource = table.getColumnSource(columnName);
        final RowSet index = usePrev ? table.getRowSet().prev() : table.getRowSet();

        final long startTime = System.currentTimeMillis();
        return ChunkedNumericalStatsKernel.makeChunkedNumericalStatsFactory(columnSource.getType())
                .processChunks(index, columnSource, usePrev)
                .setRunTime(System.currentTimeMillis()-startTime);
    }

    Result processChunks(final RowSet index, final ColumnSource<?> columnSource, boolean usePrev);


    class Result implements Serializable {
        private final long size;
        private final long count;

        private final Number sum;
        private final Number absSum;
        private final Number sqrdSum;

        private final Number min;
        private final Number max;
        private final Number absMin;
        private final Number absMax;

        private long runTime = -1;

        public Result(long size, long count, final Number sum, final Number absSum, final Number sqrdSum, final Number min, final Number max, final Number absMin, final Number absMax) {
            this.size = size;
            this.count = count;

            this.sum = sum;
            this.absSum = absSum;
            this.sqrdSum = sqrdSum;

            this.min = min;
            this.max = max;
            this.absMin = absMin;
            this.absMax = absMax;
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

        public Number getSum() {
            return sum;
        }

        public Number getAbsSum() {
            return absSum;
        }

        public Number getMin() {
            return min;
        }

        public Number getMax() {
            return max;
        }

        public Number getAbsMin() {
            return absMin;
        }

        public Number getAbsMax() {
            return absMax;
        }

        public Number getAvg() {
            return count == 0 ? Double.POSITIVE_INFINITY : sum.doubleValue() / count;
        }

        public Number getAbsAvg() {
            return count == 0 ? Double.POSITIVE_INFINITY : absSum.doubleValue() / count;
        }

        public Number getStdDev() {
            if (count <= 1) {
                return Double.NaN;
            }

            final double mean = getAvg().doubleValue();
            final double var = (sqrdSum.doubleValue() - count * mean * mean) / (count-1);
            return Math.sqrt(var);
        }

        public String getSizeString(final Format format) {
            return format.format(size);
        }

        public String getCountString(final Format format) {
            return format.format(count);
        }

        public String getSumString(final Format format) {
            return format.format(sum);
        }

        public String getAbsSumString(final Format format) {
            return format.format(absSum);
        }

        public String getAvgString(final Format format) {
            return count != 0 ? format.format(getAvg()) : "";
        }

        public String getAbsAvgString(final Format format) {
            return count != 0 ? format.format(getAbsAvg()) : "";
        }

        public String getMinString(final Format format) {
            return format.format(min);
        }

        public String getAbsMinString(final Format format) {
            return format.format(absMin);
        }

        public String getMaxString(final Format format) {
            return format.format(max);
        }

        public String getAbsMaxString(final Format format) {
            return format.format(absMax);
        }

        public String getStdDevString(final Format format) {
            if (count <= 1) {
                return "";
            }

            final Number stdDev = getStdDev();
            return stdDev != null ? format.format(stdDev) : "";
        }
    }
}
