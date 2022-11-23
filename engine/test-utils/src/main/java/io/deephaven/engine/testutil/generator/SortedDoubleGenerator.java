package io.deephaven.engine.testutil.generator;

import java.util.Random;

public class SortedDoubleGenerator extends AbstractSortedGenerator<Double> {
    private final double minValue;
    private final double maxValue;

    public SortedDoubleGenerator(double minValue, double maxValue) {
        this.minValue = minValue;
        this.maxValue = maxValue;
    }

    Double maxValue() {
        return maxValue;
    }

    Double minValue() {
        return minValue;
    }

    Double makeValue(Double floor, Double ceiling, Random random) {
        return floor + (random.nextDouble() * (ceiling - floor));
    }

    @Override
    public Class<Double> getType() {
        return Double.class;
    }
}
