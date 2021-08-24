package io.deephaven.benchmarking.generator;

import io.deephaven.benchmarking.generator.random.ExtendedRandom;

/**
 * A {@link ColumnGenerator<String>} that generates a random length constrained string.
 */
public class RandomStringColumnGenerator extends AbstractStringColumnGenerator {
    private StringGenerator generator;

    public RandomStringColumnGenerator(String name, int minLength, int maxLength) {
        super(name, minLength, maxLength);
    }

    @Override
    public void init(ExtendedRandom random) {
        generator = new StringGenerator(getMinLength(), getMaxLength(), random);
    }

    @Override
    public String get() {
        return generator.get();
    }
}
