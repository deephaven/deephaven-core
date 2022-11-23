package io.deephaven.engine.testutil.generator;

import java.util.Random;
import java.util.TreeMap;
import java.util.function.IntFunction;

public class AbstractFromUniqueGenerator<T> extends AbstractGenerator<T> {
    private final Class<T> type;
    private final AbstractUniqueGenerator<T> uniqueGenerator;
    private final AbstractGenerator<T> defaultGenerator;
    private final IntFunction<T[]> arrayFactory;
    private final double existingFraction;
    int lastSize = 0;
    T[] lastValues;

    AbstractFromUniqueGenerator(Class<T> type, AbstractUniqueGenerator<T> uniqueGenerator,
                                AbstractGenerator<T> defaultGenerator, IntFunction<T[]> arrayFactory, double existingFraction) {
        this.type = type;
        this.uniqueGenerator = uniqueGenerator;
        this.defaultGenerator = defaultGenerator;
        this.arrayFactory = arrayFactory;
        this.existingFraction = existingFraction;
    }

    @Override
    public T nextValue(TreeMap<Long, T> values, long key, Random random) {
        if (random.nextDouble() < existingFraction) {
            final int size = uniqueGenerator.getGeneratedValues().size();
            if (size != lastSize) {
                lastValues = uniqueGenerator.getGeneratedValues().stream().toArray(arrayFactory);
                lastSize = lastValues.length;
            }
            if (size > 0) {
                return lastValues[random.nextInt(lastValues.length)];
            }
        }
        return defaultGenerator.nextValue(values, key, random);
    }

    @Override
    public Class<T> getType() {
        return type;
    }
}
