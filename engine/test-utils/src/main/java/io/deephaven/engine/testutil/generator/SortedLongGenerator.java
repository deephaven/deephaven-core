package io.deephaven.engine.testutil.generator;

import io.deephaven.engine.testutil.TstUtils;

import java.util.Random;

public class SortedLongGenerator extends AbstractSortedGenerator<Long> {
    private final long minValue;
    private final long maxValue;

    public SortedLongGenerator(long minValue, long maxValue) {
        if (maxValue == Long.MAX_VALUE) {
            // Because the "range + 1" code below makes it wrap.
            throw new UnsupportedOperationException("Long.MAX_VALUE not supported");
        }
        this.minValue = minValue;
        this.maxValue = maxValue;
    }

    Long maxValue() {
        return maxValue;
    }

    Long minValue() {
        return minValue;
    }

    Long makeValue(Long floor, Long ceiling, Random random) {
        final long range = ceiling - floor;
        if (range + 1 < Integer.MAX_VALUE) {
            return floor + random.nextInt((int) (range + 1));
        }
        final int bits = 64 - Long.numberOfLeadingZeros(range);
        long candidate = TstUtils.getRandom(random, bits);
        while (candidate > range || candidate < 0) {
            candidate = TstUtils.getRandom(random, bits);
        }
        return floor + candidate;
    }

    @Override
    public Class<Long> getType() {
        return Long.class;
    }
}
