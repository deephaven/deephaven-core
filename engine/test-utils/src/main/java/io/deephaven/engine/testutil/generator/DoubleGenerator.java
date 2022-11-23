package io.deephaven.engine.testutil.generator;

import io.deephaven.base.verify.Require;
import io.deephaven.util.QueryConstants;

import java.util.Random;
import java.util.TreeMap;

public class DoubleGenerator extends AbstractGenerator<Double> {

    private final double to, from;
    private final double nullFraction;
    private final double nanFraction;
    private final double negInfFraction;
    private final double posInfFraction;

    public DoubleGenerator() {
        this(QueryConstants.NULL_DOUBLE + 1, Double.MAX_VALUE);
    }

    public DoubleGenerator(double from, double to) {
        this(from, to, 0);
    }

    public DoubleGenerator(double from, double to, double nullFraction) {
        this(from, to, nullFraction, 0.0);
    }

    public DoubleGenerator(double from, double to, double nullFraction, double nanFraction) {
        this(from, to, nullFraction, nanFraction, 0, 0);
    }

    public DoubleGenerator(double from, double to, double nullFraction, double nanFraction, double negInfFraction,
                           double posInfFraction) {
        this.from = from;
        this.to = to;
        this.nullFraction = nullFraction;
        this.nanFraction = nanFraction;
        this.negInfFraction = negInfFraction;
        this.posInfFraction = posInfFraction;
        Require.leq(nullFraction + nanFraction + negInfFraction + posInfFraction,
                "nullFraction + nanFraction + negInfFraction + posInfFraction", 1.0, "1.0");
    }

    @Override
    public Double nextValue(TreeMap<Long, Double> values, long key, Random random) {
        if (nullFraction > 0 || nanFraction > 0 || negInfFraction > 0 || posInfFraction > 0) {
            final double frac = random.nextDouble();

            if (nullFraction > 0 && frac < nullFraction) {
                return null;
            }

            if (nanFraction > 0 && frac < (nullFraction + nanFraction)) {
                return Double.NaN;
            }

            if (negInfFraction > 0 && frac < (nullFraction + nanFraction + negInfFraction)) {
                return Double.NEGATIVE_INFINITY;
            }

            if (posInfFraction > 0 && frac < (nullFraction + nanFraction + negInfFraction + posInfFraction)) {
                return Double.POSITIVE_INFINITY;
            }
        }
        return from + (random.nextDouble() * to - from);
    }

    @Override
    public Class<Double> getType() {
        return Double.class;
    }
}
