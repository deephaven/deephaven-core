package io.deephaven.engine.testutil.generator;

import java.util.Random;

public class IncreasingSortedLongGenerator extends AbstractGenerator<Long> {
    private final int step;
    private long lastValue;

    public IncreasingSortedLongGenerator(int step, long startValue) {
        this.step = step;
        this.lastValue = startValue;
    }

    @Override
    public Class<Long> getType() {
        return Long.class;
    }

    @Override
    public Long nextValue(Random random) {
        lastValue += random.nextInt(step);
        return lastValue;
    }
}
