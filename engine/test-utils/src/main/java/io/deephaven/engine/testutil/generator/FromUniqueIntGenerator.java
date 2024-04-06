//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.generator;

public class FromUniqueIntGenerator extends AbstractFromUniqueGenerator<Integer> {
    public FromUniqueIntGenerator(
            UniqueIntGenerator uniqueGenerator,
            IntGenerator defaultGenerator,
            double existingFraction) {
        super(Integer.class, uniqueGenerator, defaultGenerator, existingFraction);
    }
}
