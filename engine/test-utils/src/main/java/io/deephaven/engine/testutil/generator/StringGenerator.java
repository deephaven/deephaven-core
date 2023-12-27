package io.deephaven.engine.testutil.generator;

import io.deephaven.util.QueryConstants;

import java.util.Random;

public class StringGenerator extends AbstractGenerator<String> {
    private final int bound;
    private final double nullFrac;

    public StringGenerator() {
        this(0);
    }

    public StringGenerator(int bound) {
        this(bound, 0);
    }

    public StringGenerator(int bound, double nullFrac) {
        this.bound = bound;
        this.nullFrac = nullFrac;
    }

    @Override
    public String nextValue(Random random) {
        if (nullFrac > 0) {
            if (random.nextDouble() < nullFrac) {
                return null;
            }
        }
        final long value = bound > 0 ? random.nextInt(bound) : random.nextLong();
        return Long.toString(value, 'z' - 'a' + 10);
    }

    @Override
    public Class<String> getType() {
        return String.class;
    }
}
