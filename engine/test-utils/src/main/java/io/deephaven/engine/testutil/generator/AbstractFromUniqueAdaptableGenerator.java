//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.generator;

import java.util.Random;
import java.util.function.IntFunction;

public class AbstractFromUniqueAdaptableGenerator<SV, CV> extends AbstractGenerator<CV> {

    private final Class<CV> type;
    private final AbstractAdaptableUniqueGenerator<SV, CV> uniqueGenerator;
    private final AbstractGenerator<CV> defaultGenerator;
    private final IntFunction<SV[]> arrayFactory;
    private final double existingFraction;
    int lastSize = 0;
    SV[] lastValues;

    AbstractFromUniqueAdaptableGenerator(Class<CV> type, AbstractAdaptableUniqueGenerator<SV, CV> uniqueGenerator,
            AbstractGenerator<CV> defaultGenerator, IntFunction<SV[]> arrayFactory, double existingFraction) {
        this.type = type;
        this.uniqueGenerator = uniqueGenerator;
        this.defaultGenerator = defaultGenerator;
        this.arrayFactory = arrayFactory;
        this.existingFraction = existingFraction;
    }

    @Override
    public CV nextValue(Random random) {
        if (random.nextDouble() < existingFraction) {
            final int size = uniqueGenerator.getGeneratedValues().size();
            if (size != lastSize) {
                lastValues = uniqueGenerator.getGeneratedValues().toArray(arrayFactory);
                lastSize = lastValues.length;
            }

            if (size > 0) {
                return uniqueGenerator.adapt(lastValues[random.nextInt(lastValues.length)]);
            }
        }
        return defaultGenerator.nextValue(random);
    }

    @Override
    public Class<CV> getType() {
        return type;
    }
}
