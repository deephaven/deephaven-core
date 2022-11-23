package io.deephaven.engine.testutil.generator;

import io.deephaven.tuple.ArrayTuple;

public class FromUniqueTupleGenerator extends AbstractFromUniqueGenerator<ArrayTuple> {
    public FromUniqueTupleGenerator(UniqueTupleGenerator uniqueGenerator, TupleGenerator defaultGenerator,
                                    double existingFraction) {
        super(ArrayTuple.class, uniqueGenerator, defaultGenerator, ArrayTuple[]::new, existingFraction);
    }
}
