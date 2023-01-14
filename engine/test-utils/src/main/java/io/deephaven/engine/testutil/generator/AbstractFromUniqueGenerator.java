package io.deephaven.engine.testutil.generator;

import java.util.Random;

public class AbstractFromUniqueGenerator<T> extends AbstractGenerator<T> {
    private final Class<T> type;
    private final UniqueTestDataGenerator<T, T> uniqueGenerator;
    private final AbstractGenerator<T> defaultGenerator;
    private final double existingFraction;

    AbstractFromUniqueGenerator(
            Class<T> type,
            UniqueTestDataGenerator<T, T> uniqueGenerator,
            AbstractGenerator<T> defaultGenerator,
            double existingFraction) {
        this.type = type;
        this.uniqueGenerator = uniqueGenerator;
        this.defaultGenerator = defaultGenerator;
        this.existingFraction = existingFraction;
    }

    @Override
    public T nextValue(Random random) {
        if (random.nextDouble() < existingFraction) {
            if (uniqueGenerator.hasValues()) {
                return uniqueGenerator.getRandomValue(random);
            }
        }
        return defaultGenerator.nextValue(random);
    }

    @Override
    public Class<T> getType() {
        return type;
    }
}
