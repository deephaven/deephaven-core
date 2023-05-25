package io.deephaven.engine.testutil.generator;

import io.deephaven.base.verify.Assert;

import java.math.BigInteger;
import java.util.Random;

public class BigIntegerGenerator extends AbstractGenerator<BigInteger> {
    private static final BigInteger DEFAULT_FROM = BigInteger.valueOf(Long.MIN_VALUE).multiply(BigInteger.valueOf(2));
    private static final BigInteger DEFAULT_TO = BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(2));

    private final BigInteger to, from;
    private final double nullFraction;
    private final int rangeBitLength;
    private final BigInteger range;

    public BigIntegerGenerator() {
        this(DEFAULT_FROM, DEFAULT_TO);
    }

    public BigIntegerGenerator(double nullFraction) {
        this(BigInteger.valueOf(Long.MIN_VALUE).multiply(BigInteger.valueOf(2)),
                BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(2)), 0);
    }

    public BigIntegerGenerator(BigInteger from, BigInteger to) {
        this(from, to, 0);
    }

    public BigIntegerGenerator(BigInteger from, BigInteger to, double nullFraction) {
        this.from = from;
        this.to = to;
        this.nullFraction = nullFraction;

        range = to.subtract(from);
        rangeBitLength = range.bitLength();
    }

    @Override
    public BigInteger nextValue(Random random) {
        if (nullFraction > 0) {
            if (random.nextDouble() < nullFraction) {
                return null;
            }
        }

        BigInteger value;
        do {
            value = new BigInteger(rangeBitLength, random);
        } while (value.compareTo(range) > 0);

        final BigInteger result = value.add(from);

        Assert.assertion(result.compareTo(from) >= 0, "result.compareTo(from) >= 0", result, "result", from, "from");
        Assert.assertion(result.compareTo(to) <= 0, "result.compareTo(to) <= 0", result, "result", to, "to");

        return result;
    }

    @Override
    public Class<BigInteger> getType() {
        return BigInteger.class;
    }
}
