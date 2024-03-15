//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.generator;

import io.deephaven.tuple.ArrayTuple;

import java.util.Arrays;
import java.util.Random;

public class ArrayTupleGenerator extends AbstractGenerator<ArrayTuple> {
    private final AbstractGenerator<?>[] generators;

    public ArrayTupleGenerator(AbstractGenerator<?>... generators) {
        this.generators = generators;
    }

    @Override
    public ArrayTuple nextValue(Random random) {
        return new ArrayTuple(Arrays.stream(generators).map(g -> g.nextValue(random)).toArray());
    }

    @Override
    public Class<ArrayTuple> getType() {
        return ArrayTuple.class;
    }
}
