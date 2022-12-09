package io.deephaven.engine.testutil.generator;

import java.util.Random;

public class SortedIntGenerator extends AbstractSortedGenerator<Integer> {
    private final Integer minInteger;
    private final Integer maxInteger;

    public SortedIntGenerator(Integer minInteger, Integer maxInteger) {
        this.minInteger = minInteger;
        this.maxInteger = maxInteger;
    }

    Integer maxValue() {
        return maxInteger;
    }

    Integer minValue() {
        return minInteger;
    }

    Integer makeValue(Integer floor, Integer ceiling, Random random) {
        return floor + random.nextInt(ceiling - floor + 1);
    }

    @Override
    public Class<Integer> getType() {
        return Integer.class;
    }
}
