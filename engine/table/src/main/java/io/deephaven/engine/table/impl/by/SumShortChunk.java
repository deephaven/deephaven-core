//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit SumCharChunk and run "./gradlew replicateOperators" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.by;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.util.QueryConstants;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.util.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableDouble;

public class SumShortChunk {
    private SumShortChunk() {} // static use only

    static long sumShortChunk(ShortChunk<? extends Any> values, int chunkStart, int chunkSize, MutableInt nonNullCount) {
        final int end = chunkStart + chunkSize;
        long sum = 0;
        for (int ii = chunkStart; ii < end; ++ii) {
            final short value = values.get(ii);
            if (value != QueryConstants.NULL_SHORT) {
                sum += value;
                nonNullCount.increment();
            }
        }
        return sum;
    }

    public static long sumShortChunk(ShortChunk<? extends Any> values, int chunkStart, int chunkSize) {
        final int end = chunkStart + chunkSize;
        long sum = 0;
        for (int ii = chunkStart; ii < end; ++ii) {
            final short value = values.get(ii);
            if (value != QueryConstants.NULL_SHORT) {
                sum += value;
            }
        }
        return sum;
    }

    /**
     * Produce the sum and sum of squares of a shortacter chunk, as doubles.
     */
    static double sum2ShortChunk(ShortChunk<? extends Any> values, int chunkStart, int chunkSize, MutableInt nonNullCount,
            MutableDouble sum2out) {
        final int end = chunkStart + chunkSize;
        double sum = 0;
        double sum2 = 0;
        for (int ii = chunkStart; ii < end; ++ii) {
            final short value = values.get(ii);
            if (value != QueryConstants.NULL_SHORT) {
                // noinspection UnnecessaryLocalVariable
                final double doubleValue = value;
                sum += doubleValue;
                sum2 += doubleValue * doubleValue;
                nonNullCount.increment();
            }
        }

        sum2out.setValue(sum2);
        return sum;
    }

    static private short abs(short val) {
        if (val == QueryConstants.NULL_SHORT) {
            return val;
        } else if (val < 0) {
            return (short) -val;
        } else {
            return val;
        }
    }

    static long sumShortChunkAbs(ShortChunk<? extends Any> values, int chunkStart, int chunkSize,
            MutableInt nonNullCount) {
        final int end = chunkStart + chunkSize;
        long sum = 0;
        for (int ii = chunkStart; ii < end; ++ii) {
            final short value = abs(values.get(ii));
            if (value != QueryConstants.NULL_SHORT) {
                sum += value;
                nonNullCount.increment();
            }
        }
        return sum;
    }
}
