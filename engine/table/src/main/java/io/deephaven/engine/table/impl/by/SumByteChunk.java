/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit SumCharChunk and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.by;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.util.QueryConstants;
import io.deephaven.chunk.ByteChunk;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.mutable.MutableInt;

public class SumByteChunk {
    private SumByteChunk() {} // static use only

    static long sumByteChunk(ByteChunk<? extends Any> values, int chunkStart, int chunkSize, MutableInt nonNullCount) {
        final int end = chunkStart + chunkSize;
        long sum = 0;
        for (int ii = chunkStart; ii < end; ++ii) {
            final byte value = values.get(ii);
            if (value != QueryConstants.NULL_BYTE) {
                sum += value;
                nonNullCount.increment();
            }
        }
        return sum;
    }

    public static long sumByteChunk(ByteChunk<? extends Any> values, int chunkStart, int chunkSize) {
        final int end = chunkStart + chunkSize;
        long sum = 0;
        for (int ii = chunkStart; ii < end; ++ii) {
            final byte value = values.get(ii);
            if (value != QueryConstants.NULL_BYTE) {
                sum += value;
            }
        }
        return sum;
    }

    /**
     * Produce the sum and sum of squares of a byteacter chunk, as doubles.
     */
    static double sum2ByteChunk(ByteChunk<? extends Any> values, int chunkStart, int chunkSize, MutableInt nonNullCount, MutableDouble sum2out) {
        final int end = chunkStart + chunkSize;
        double sum = 0;
        double sum2 = 0;
        for (int ii = chunkStart; ii < end; ++ii) {
            final byte value = values.get(ii);
            if (value != QueryConstants.NULL_BYTE) {
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

    static private byte abs(byte val) {
        if (val == QueryConstants.NULL_BYTE) {
            return val;
        } else if (val < 0) {
            return (byte)-val;
        } else {
            return val;
        }
    }

    static long sumByteChunkAbs(ByteChunk<? extends Any> values, int chunkStart, int chunkSize, MutableInt nonNullCount) {
        final int end = chunkStart + chunkSize;
        long sum = 0;
        for (int ii = chunkStart; ii < end; ++ii) {
            final byte value = abs(values.get(ii));
            if (value != QueryConstants.NULL_BYTE) {
                sum += value;
                nonNullCount.increment();
            }
        }
        return sum;
    }
}
