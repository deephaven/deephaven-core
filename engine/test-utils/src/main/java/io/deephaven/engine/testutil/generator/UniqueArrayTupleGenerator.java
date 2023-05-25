package io.deephaven.engine.testutil.generator;

import io.deephaven.tuple.ArrayTuple;

import java.util.Arrays;
import java.util.Random;

/**
 * Generates unique ArrayTuples for table test columns.
 *
 * The ArrayTuple is derived from a set of AbstractGenerators
 */
public class UniqueArrayTupleGenerator extends AbstractUniqueGenerator<ArrayTuple> {
    private final AbstractGenerator<?>[] generators;

    public UniqueArrayTupleGenerator(AbstractGenerator<?>... generators) {
        this.generators = generators;
    }

    @Override
    public ArrayTuple nextValue(long key, Random random) {
        return new ArrayTuple(Arrays.stream(generators).map(g -> g.nextValue(random)).toArray());
    }

    @Override
    public Class<ArrayTuple> getType() {
        return ArrayTuple.class;
    }
}
