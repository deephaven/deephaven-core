package io.deephaven.engine.testutil.generator;

import io.deephaven.util.QueryConstants;

import java.util.Random;
import java.util.TreeMap;

public class LongGenerator extends AbstractGenerator<Long> {

    private final long to, from;
    private final double nullFraction;

    public LongGenerator() {
        this(QueryConstants.NULL_LONG + 1, Long.MAX_VALUE);
    }

    public LongGenerator(long from, long to) {
        this(from, to, 0.0);
    }

    public LongGenerator(long from, long to, double nullFraction) {
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
        final long distance = to - from;
        if (distance > 0 && distance < Integer.MAX_VALUE) {
            return from + random.nextInt((int) (to - from));
        } else if (from == QueryConstants.NULL_LONG + 1 && to == Long.MAX_VALUE) {
            long r;
            do {
                r = random.nextLong();
            } while (r == QueryConstants.NULL_LONG);
            return r;
        } else {
            return (long) (from + random.nextDouble() * (to - from));
        }
    }

    @Override
    public Class<Long> getType() {
        return Long.class;
    }
}
