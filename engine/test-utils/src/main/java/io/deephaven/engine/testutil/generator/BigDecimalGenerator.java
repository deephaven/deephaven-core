package io.deephaven.engine.testutil.generator;

import io.deephaven.base.verify.Assert;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Random;

public class BigDecimalGenerator extends AbstractGenerator<BigDecimal> {

    static final BigInteger DEFAULT_FROM = BigInteger.valueOf(Long.MIN_VALUE).multiply(BigInteger.valueOf(2));
    static final BigInteger DEFAULT_TO = BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(2));
    private final BigInteger to, from;
    private final BigDecimal toDecimal, fromDecimal;
    private final double nullFraction;
    private final int rangeBitLength;
    private final int decimalPlaces;
    private final BigInteger range;

    public BigDecimalGenerator() {
        this(DEFAULT_FROM, DEFAULT_TO, 10, 0);
    }

    public BigDecimalGenerator(double nullFraction) {
        this(DEFAULT_FROM, DEFAULT_TO, 10, nullFraction);
    }

    public BigDecimalGenerator(BigInteger from, BigInteger to) {
        this(from, to, 10, 0);
    }

    public BigDecimalGenerator(BigInteger from, BigInteger to, int decimalPlaces, double nullFraction) {
        this.from = from;
        this.to = to;
        this.nullFraction = nullFraction;
        final BigInteger scale = BigInteger.TEN.pow(decimalPlaces);
        this.decimalPlaces = decimalPlaces;

        range = to.subtract(from).multiply(scale);
        rangeBitLength = range.bitLength();

        toDecimal = new BigDecimal(to);
        fromDecimal = new BigDecimal(from);
    }

    @Override
    public BigDecimal nextValue(Random random) {
        if (nullFraction > 0) {
            if (random.nextDouble() < nullFraction) {
                return null;
            }
        }

        BigInteger value;
        do {
            value = new BigInteger(rangeBitLength, random);
        } while (value.compareTo(range) > 0);

        final BigDecimal result = new BigDecimal(value, decimalPlaces).add(fromDecimal);

        Assert.assertion(result.compareTo(fromDecimal) >= 0, "result.compareTo(from) >= 0", result, "result", from,
                "from");
        Assert.assertion(result.compareTo(toDecimal) <= 0, "result.compareTo(to) <= 0", result, "result", to, "to");

        return result;
    }

    @Override
    public Class<BigDecimal> getType() {
        return BigDecimal.class;
    }
}
