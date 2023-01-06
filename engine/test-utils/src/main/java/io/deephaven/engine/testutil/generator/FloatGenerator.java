package io.deephaven.engine.testutil.generator;

import io.deephaven.base.verify.Require;
import io.deephaven.util.QueryConstants;

import java.util.Random;
import java.util.TreeMap;

public class FloatGenerator extends AbstractGenerator<Float> {

    private final float to, from;
    private final double nullFraction;
    private final double nanFraction;
    private final double negInfFraction;
    private final double posInfFraction;

    public FloatGenerator() {
        this(QueryConstants.NULL_FLOAT + 1, Float.MAX_VALUE);
    }

    public FloatGenerator(float from, float to) {
        this(from, to, 0);
    }

    public FloatGenerator(float from, float to, double nullFraction) {
        this(from, to, nullFraction, 0.0);
    }

    public FloatGenerator(float from, float to, double nullFraction, double nanFraction) {
        this(from, to, nullFraction, nanFraction, 0, 0);
    }

    public FloatGenerator(float from, float to, double nullFraction, double nanFraction, double negInfFraction,
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
    public Float nextValue(TreeMap<Long, Float> values, long key, Random random) {
        if (nullFraction > 0 || nanFraction > 0 || negInfFraction > 0 || posInfFraction > 0) {
            final double frac = random.nextDouble();

            if (nullFraction > 0 && frac < nullFraction) {
                return null;
            }

            if (nanFraction > 0 && frac < (nullFraction + nanFraction)) {
                return Float.NaN;
            }

            if (negInfFraction > 0 && frac < (nullFraction + nanFraction + negInfFraction)) {
                return Float.NEGATIVE_INFINITY;
            }

            if (posInfFraction > 0 && frac < (nullFraction + nanFraction + negInfFraction + posInfFraction)) {
                return Float.POSITIVE_INFINITY;
            }
        }
        return from + (random.nextFloat() * to - from);
    }

    @Override
    public Class<Float> getType() {
        return Float.class;
    }
}
