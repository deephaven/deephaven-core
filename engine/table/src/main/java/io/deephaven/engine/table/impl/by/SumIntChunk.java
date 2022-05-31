/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit SumCharChunk and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.by;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.util.QueryConstants;
import io.deephaven.chunk.IntChunk;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.mutable.MutableInt;

public class SumIntChunk {
    private SumIntChunk() {} // static use only

    static long sumIntChunk(IntChunk<? extends Any> values, int chunkStart, int chunkSize, MutableInt nonNullCount) {
        final int end = chunkStart + chunkSize;
        long sum = 0;
        for (int ii = chunkStart; ii < end; ++ii) {
            final int value = values.get(ii);
            if (value != QueryConstants.NULL_INT) {
                sum += value;
                nonNullCount.increment();
            }
        }
        return sum;
    }

    public static long sumIntChunk(IntChunk<? extends Any> values, int chunkStart, int chunkSize) {
        final int end = chunkStart + chunkSize;
        long sum = 0;
        for (int ii = chunkStart; ii < end; ++ii) {
            final int value = values.get(ii);
            if (value != QueryConstants.NULL_INT) {
                sum += value;
            }
        }
        return sum;
    }

    /**
     * Produce the sum and sum of squares of a intacter chunk, as doubles.
     */
    static double sum2IntChunk(IntChunk<? extends Any> values, int chunkStart, int chunkSize, MutableInt nonNullCount, MutableDouble sum2out) {
        final int end = chunkStart + chunkSize;
        double sum = 0;
        double sum2 = 0;
        for (int ii = chunkStart; ii < end; ++ii) {
            final int value = values.get(ii);
            if (value != QueryConstants.NULL_INT) {
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

    static private int abs(int val) {
        if (val == QueryConstants.NULL_INT) {
            return val;
        } else if (val < 0) {
            return (int)-val;
        } else {
            return val;
        }
    }

    static long sumIntChunkAbs(IntChunk<? extends Any> values, int chunkStart, int chunkSize, MutableInt nonNullCount) {
        final int end = chunkStart + chunkSize;
        long sum = 0;
        for (int ii = chunkStart; ii < end; ++ii) {
            final int value = abs(values.get(ii));
            if (value != QueryConstants.NULL_INT) {
                sum += value;
                nonNullCount.increment();
            }
        }
        return sum;
    }
}
