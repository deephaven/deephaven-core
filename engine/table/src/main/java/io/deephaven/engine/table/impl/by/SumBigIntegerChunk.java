package io.deephaven.engine.table.impl.by;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableObject;

import java.math.BigInteger;

class SumBigIntegerChunk  {
    private SumBigIntegerChunk() {} // static use only

    static BigInteger sumBigIntegerChunk(ObjectChunk<BigInteger, ? extends Values> values, int chunkStart, int chunkSize, MutableInt chunkNonNull) {
        BigInteger partialSum = BigInteger.ZERO;

        for (int ii = chunkStart; ii < chunkStart + chunkSize; ++ii) {
            final BigInteger value = values.get(ii);
            if (value != null) {
                chunkNonNull.increment();
                partialSum = partialSum.add(value);
            }
        }
        return partialSum;
    }

    static BigInteger sumBigIntegerChunkAbs(ObjectChunk<BigInteger, ? extends Values> values, int chunkStart, int chunkSize, MutableInt chunkNonNull) {
        BigInteger partialSum = BigInteger.ZERO;

        for (int ii = chunkStart; ii < chunkStart + chunkSize; ++ii) {
            final BigInteger value = values.get(ii);
            if (value != null) {
                chunkNonNull.increment();
                partialSum = partialSum.add(value.abs());
            }
        }
        return partialSum;
    }

    static BigInteger sum2BigIntegerChunk(ObjectChunk<BigInteger, ? extends Values> values, int chunkStart, int chunkSize, MutableInt chunkNonNull, MutableObject<BigInteger> sum2out) {
        final int end = chunkStart + chunkSize;
        BigInteger sum = BigInteger.ZERO;
        BigInteger sum2 = BigInteger.ZERO;
        for (int ii = chunkStart; ii < end; ++ii) {
            final BigInteger value = values.get(ii);
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
