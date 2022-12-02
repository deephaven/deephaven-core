package io.deephaven.engine.testutil.generator;

import io.deephaven.engine.testutil.generator.AbstractGenerator;

import java.util.Random;
import java.util.TreeMap;

public class BooleanGenerator extends AbstractGenerator<Boolean> {

    private final double trueFraction;
    private final double nullFraction;

    public BooleanGenerator() {
        this(0.5, 0);
    }

    public BooleanGenerator(double trueFraction) {
        this(trueFraction, 0);
    }

    public BooleanGenerator(double trueFraction, double nullFraction) {
        this.trueFraction = trueFraction;
        this.nullFraction = nullFraction;
    }

    @Override
    public Boolean nextValue(TreeMap<Long, Boolean> values, long key, Random random) {
        if (nullFraction > 0) {
            if (random.nextDouble() < nullFraction) {
                return null;
            }
        }
        return random.nextDouble() < trueFraction;
    }

    @Override
    public Class<Boolean> getType() {
        return Boolean.class;
    }
}
