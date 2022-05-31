package io.deephaven.engine.table.impl.by;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.QueryConstants;
import io.deephaven.chunk.FloatChunk;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.mutable.MutableInt;

class SumFloatChunk {
    private SumFloatChunk() {} // static use only

    static double sumFloatChunk(FloatChunk<? extends Values> values, int chunkStart, int chunkSize,
                                MutableInt chunkNormalCount,
                                MutableInt chunkNanCount,
                                MutableInt chunkInfinityCount,
                                MutableInt chunkMinusInfinityCount) {
        final int end = chunkStart + chunkSize;
        double sum = 0;
        for (int ii = chunkStart; ii < end; ++ii) {
            final float aFloat = values.get(ii);

            if (Float.isNaN(aFloat)) {
                chunkNanCount.increment();
            } else if (aFloat == Float.POSITIVE_INFINITY) {
                chunkInfinityCount.increment();
            } else if (aFloat == Float.NEGATIVE_INFINITY) {
                chunkMinusInfinityCount.increment();
            } else if (!(aFloat == QueryConstants.NULL_FLOAT)) {
                sum += aFloat;
                chunkNormalCount.increment();
            }
        }
        return sum;
    }

    static double sum2FloatChunk(FloatChunk<? extends Values> values, int chunkStart, int chunkSize,
                                 MutableInt chunkNormalCount,
                                 MutableInt chunkNanCount,
                                 MutableInt chunkInfinityCount,
                                 MutableInt chunkMinusInfinityCount,
                                 MutableDouble sum2out) {
        final int end = chunkStart + chunkSize;
        double sum = 0;
        double sum2 = 0;

        for (int ii = chunkStart; ii < end; ++ii) {
            final float value = values.get(ii);
            if (value != QueryConstants.NULL_FLOAT) {
                if (Float.isNaN(value)) {
                    chunkNanCount.increment();
                } else if (value == Float.POSITIVE_INFINITY) {
                    chunkInfinityCount.increment();
                } else if (value == Float.NEGATIVE_INFINITY) {
                    chunkMinusInfinityCount.increment();
                } else {
                    sum += value;
                    sum2 += (double) value * (double) value;
                    chunkNormalCount.increment();
                }
            }
        }

        sum2out.setValue(sum2);

        return sum;
    }

    static double sumFloatChunkAbs(FloatChunk<? extends Values> values, int chunkStart, int chunkSize,
                                   MutableInt chunkNormalCount,
                                   MutableInt chunkNanCount,
                                   MutableInt chunkInfinityCount) {
        final int end = chunkStart + chunkSize;
        double sum = 0;
        for (int ii = chunkStart; ii < end; ++ii) {
            final float aFloat = values.get(ii);

            if (Float.isNaN(aFloat)) {
                chunkNanCount.increment();
            } else if (aFloat == Float.POSITIVE_INFINITY || aFloat == Float.NEGATIVE_INFINITY) {
                chunkInfinityCount.increment();
            } else if (!(aFloat == QueryConstants.NULL_FLOAT)) {
                sum += Math.abs(aFloat);
                chunkNormalCount.increment();
            }
        }
        return sum;
    }
}
