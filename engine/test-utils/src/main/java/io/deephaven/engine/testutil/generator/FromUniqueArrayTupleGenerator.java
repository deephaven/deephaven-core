package io.deephaven.engine.testutil.generator;

import io.deephaven.tuple.ArrayTuple;

public class FromUniqueArrayTupleGenerator extends AbstractFromUniqueGenerator<ArrayTuple> {
    public FromUniqueArrayTupleGenerator(
            UniqueArrayTupleGenerator uniqueGenerator,
            ArrayTupleGenerator defaultGenerator,
            double existingFraction) {
        super(ArrayTuple.class, uniqueGenerator, defaultGenerator, existingFraction);
    }
}
