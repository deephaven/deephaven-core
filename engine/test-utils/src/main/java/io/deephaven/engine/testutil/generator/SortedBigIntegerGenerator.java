//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.generator;

import java.math.BigInteger;
import java.util.Random;

public class SortedBigIntegerGenerator extends AbstractSortedGenerator<BigInteger> {
    private final BigInteger minValue;
    private final BigInteger maxValue;

    public SortedBigIntegerGenerator(int numBits) {
        minValue = BigInteger.ZERO;
        BigInteger mv = BigInteger.ZERO;
        mv = mv.setBit(numBits + 1).subtract(BigInteger.ONE);
        maxValue = mv;
    }

    @Override
    BigInteger minValue() {
        return minValue;
    }

    @Override
    BigInteger maxValue() {
        return maxValue;
    }

    @Override
    BigInteger makeValue(BigInteger floor, BigInteger ceiling, Random random) {
        final BigInteger range = ceiling.subtract(floor);
        final int allowedBits = range.bitLength();
        BigInteger candidate = null;
        for (int ii = 0; ii < 100; ++ii) {
            candidate = new BigInteger(allowedBits, random);
            if (candidate.compareTo(range) < 0) {
                break;
            }
            candidate = null;
        }
        if (candidate == null) {
            throw new RuntimeException(String.format("Couldn't find a suitable BigInteger between %s and %s",
                    floor, ceiling));
        }
        return floor.add(candidate);
    }

    @Override
    public Class<BigInteger> getType() {
        return BigInteger.class;
    }
}
