package io.deephaven.engine.testutil.generator;

import java.util.Random;
import java.util.TreeMap;

public class UniqueCharGenerator extends AbstractUniqueGenerator<Character> {
    private final char to, from;
    private final double nullFraction;

    public UniqueCharGenerator(char from, char to) {
        this(from, to, 0.0);
    }

    public UniqueCharGenerator(char from, char to, double nullFraction) {
        this.from = from;
        this.to = to;
        this.nullFraction = nullFraction;
    }

    @Override
    public Character nextValue(TreeMap<Long, Character> values, long key, Random random) {
        if (nullFraction > 0) {
            if (random.nextDouble() < nullFraction) {
                return null;
            }
        }

        return (char) (from + random.nextInt(to - from));
    }

    @Override
    public Class<Character> getType() {
        return Character.class;
    }
}
