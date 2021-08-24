package io.deephaven.db.v2.by;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ObjectChunk;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableObject;

import java.math.BigDecimal;

class SumBigDecimalChunk {
    private SumBigDecimalChunk() {} // static use only

    static BigDecimal sumBigDecimalChunk(
        ObjectChunk<BigDecimal, ? extends Attributes.Values> values, int chunkStart, int chunkSize,
        MutableInt chunkNonNull) {
        BigDecimal partialSum = BigDecimal.ZERO;
        for (int ii = chunkStart; ii < chunkStart + chunkSize; ++ii) {
            final BigDecimal value = values.get(ii);
            if (value != null) {
                chunkNonNull.increment();
                partialSum = partialSum.add(value);
            }
        }
        return partialSum;
    }

    static BigDecimal sumBigDecimalChunkAbs(
        ObjectChunk<BigDecimal, ? extends Attributes.Values> values, int chunkStart, int chunkSize,
        MutableInt chunkNonNull) {
        BigDecimal partialSum = BigDecimal.ZERO;
        for (int ii = chunkStart; ii < chunkStart + chunkSize; ++ii) {
            final BigDecimal value = values.get(ii);
            if (value != null) {
                chunkNonNull.increment();
                partialSum = partialSum.add(value.abs());
            }
        }
        return partialSum;
    }

    static BigDecimal sum2BigDecimalChunk(
        ObjectChunk<BigDecimal, ? extends Attributes.Values> values, int chunkStart, int chunkSize,
        MutableInt chunkNonNull, MutableObject<BigDecimal> sum2out) {
        final int end = chunkStart + chunkSize;
        BigDecimal sum = BigDecimal.ZERO;
        BigDecimal sum2 = BigDecimal.ZERO;
        for (int ii = chunkStart; ii < end; ++ii) {
            final BigDecimal value = values.get(ii);
            if (value != null) {
                sum = sum.add(value);
                sum2 = sum2.add(value.pow(2));
                chunkNonNull.increment();
            }
        }
        sum2out.setValue(sum2);
        return sum;
    }
}
