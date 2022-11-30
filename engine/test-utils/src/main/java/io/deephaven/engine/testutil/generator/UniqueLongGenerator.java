package io.deephaven.engine.testutil.generator;

import java.util.Random;
import java.util.TreeMap;

public class UniqueLongGenerator extends AbstractUniqueGenerator<Long> {

    private final int to, from;
    private final double nullFraction;

    public UniqueLongGenerator(int from, int to) {
        this(from, to, 0.0);
    }

    public UniqueLongGenerator(int from, int to, double nullFraction) {
        this.from = from;
        this.to = to;
        this.nullFraction = nullFraction;
    }

    @Override
    public Long nextValue(TreeMap<Long, Long> values, long key, Random random) {
        if (nullFraction > 0) {
            if (random.nextDouble() < nullFraction) {
                return null;
            }
        }

        return (long) (from + random.nextInt(to - from));
    }

    @Override
    public Class<Long> getType() {
        return Long.class;
    }
}
