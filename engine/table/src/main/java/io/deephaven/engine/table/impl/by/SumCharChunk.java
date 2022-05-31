package io.deephaven.engine.table.impl.by;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.util.QueryConstants;
import io.deephaven.chunk.CharChunk;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.mutable.MutableInt;

public class SumCharChunk {
    private SumCharChunk() {} // static use only

    static long sumCharChunk(CharChunk<? extends Any> values, int chunkStart, int chunkSize, MutableInt nonNullCount) {
        final int end = chunkStart + chunkSize;
        long sum = 0;
        for (int ii = chunkStart; ii < end; ++ii) {
            final char value = values.get(ii);
            if (value != QueryConstants.NULL_CHAR) {
                sum += value;
                nonNullCount.increment();
            }
        }
        return sum;
    }

    public static long sumCharChunk(CharChunk<? extends Any> values, int chunkStart, int chunkSize) {
        final int end = chunkStart + chunkSize;
        long sum = 0;
        for (int ii = chunkStart; ii < end; ++ii) {
            final char value = values.get(ii);
            if (value != QueryConstants.NULL_CHAR) {
                sum += value;
            }
        }
        return sum;
    }

    /**
     * Produce the sum and sum of squares of a character chunk, as doubles.
     */
    static double sum2CharChunk(CharChunk<? extends Any> values, int chunkStart, int chunkSize, MutableInt nonNullCount, MutableDouble sum2out) {
        final int end = chunkStart + chunkSize;
        double sum = 0;
        double sum2 = 0;
        for (int ii = chunkStart; ii < end; ++ii) {
            final char value = values.get(ii);
            if (value != QueryConstants.NULL_CHAR) {
                //noinspection UnnecessaryLocalVariable
                final double doubleValue = value;
                sum += doubleValue;
                sum2 += doubleValue * doubleValue;
                nonNullCount.increment();
            }
        }

        sum2out.setValue(sum2);
        return sum;
    }

    static private char abs(char val) {
        if (val == QueryConstants.NULL_CHAR) {
            return val;
        } else if (val < 0) {
            return (char)-val;
        } else {
            return val;
        }
    }

    static long sumCharChunkAbs(CharChunk<? extends Any> values, int chunkStart, int chunkSize, MutableInt nonNullCount) {
        final int end = chunkStart + chunkSize;
        long sum = 0;
        for (int ii = chunkStart; ii < end; ++ii) {
            final char value = abs(values.get(ii));
            if (value != QueryConstants.NULL_CHAR) {
                sum += value;
                nonNullCount.increment();
            }
        }
        return sum;
    }
}
