package io.deephaven.engine.testutil.generator;

import io.deephaven.tuple.ArrayTuple;

import java.util.Arrays;
import java.util.Random;
import java.util.TreeMap;

public class TupleGenerator extends AbstractGenerator<ArrayTuple> {
    private final AbstractGenerator[] generators;

    public TupleGenerator(AbstractGenerator... generators) {
        this.generators = generators;
    }

    @Override
    public ArrayTuple nextValue(TreeMap<Long, ArrayTuple> values, long key, Random random) {
        // noinspection unchecked
        return new ArrayTuple(Arrays.stream(generators).map(g -> g.nextValue(null, key, random)).toArray());
    }

    @Override
    public Class<ArrayTuple> getType() {
        return ArrayTuple.class;
    }
}
