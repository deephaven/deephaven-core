package io.deephaven.engine.testutil.generator;

import java.util.Random;
import java.util.TreeMap;

public class UniqueIntGenerator extends AbstractUniqueGenerator<Integer> {

    private final int to, from;
    private final double nullFraction;

    public UniqueIntGenerator(int from, int to) {
        this(from, to, 0.0);
    }

    public UniqueIntGenerator(int from, int to, double nullFraction) {
        this.from = from;
        this.to = to;
        this.nullFraction = nullFraction;
    }

    @Override
    public Integer nextValue(TreeMap<Long, Integer> values, long key, Random random) {
        if (nullFraction > 0) {
            if (random.nextDouble() < nullFraction) {
                return null;
            }
        }

        return from + random.nextInt(to - from);
    }

    @Override
    public Class<Integer> getType() {
        return Integer.class;
    }
}
