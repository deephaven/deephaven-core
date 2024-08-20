//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.generator;

import java.util.List;

public class FromUniqueStringArrayGenerator extends AbstractFromUniqueAdaptableGenerator<List<String>, String[]> {
    public FromUniqueStringArrayGenerator(
            UniqueStringArrayGenerator uniqueStringArrayGenerator,
            StringArrayGenerator defaultGenerator,
            double existingFraction) {
        super(String[].class, uniqueStringArrayGenerator, defaultGenerator, List[]::new, existingFraction);
    }
}

