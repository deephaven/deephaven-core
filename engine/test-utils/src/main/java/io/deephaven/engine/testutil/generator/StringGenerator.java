package io.deephaven.engine.testutil.generator;

import java.util.Random;

public class StringGenerator extends AbstractGenerator<String> {
    int bound;

    public StringGenerator() {
        bound = 0;
    }

    public StringGenerator(int bound) {
        this.bound = bound;
    }

    @Override
    public String nextValue(Random random) {
        final long value = bound > 0 ? random.nextInt(bound) : random.nextLong();
        return Long.toString(value, 'z' - 'a' + 10);
    }

    @Override
    public Class<String> getType() {
        return String.class;
    }
}
