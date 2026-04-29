//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.generator;

import it.unimi.dsi.fastutil.ints.IntArrayList;

public class FromUniqueIntArrayGenerator extends AbstractFromUniqueAdaptableGenerator<IntArrayList, int[]> {
    public FromUniqueIntArrayGenerator(
            UniqueIntArrayGenerator uniqueStringArrayGenerator,
            IntArrayGenerator defaultGenerator,
            double existingFraction) {
        super(int[].class, uniqueStringArrayGenerator, defaultGenerator, IntArrayList[]::new, existingFraction);
    }
}
