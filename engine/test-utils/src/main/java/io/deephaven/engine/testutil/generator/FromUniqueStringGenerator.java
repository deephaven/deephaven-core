//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.generator;

public class FromUniqueStringGenerator extends AbstractFromUniqueGenerator<String> {
    public FromUniqueStringGenerator(
            UniqueStringGenerator uniqueStringGenerator,
            double existingFraction) {
        this(uniqueStringGenerator, existingFraction, new StringGenerator());
    }

    FromUniqueStringGenerator(
            UniqueStringGenerator uniqueGenerator,
            double existingFraction,
            AbstractGenerator<String> defaultGenerator) {
        super(String.class, uniqueGenerator, defaultGenerator, existingFraction);
    }
}
