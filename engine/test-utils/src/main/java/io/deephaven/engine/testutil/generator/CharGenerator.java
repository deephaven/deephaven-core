package io.deephaven.engine.testutil.generator;

import java.util.Random;
import java.util.TreeMap;

public class CharGenerator extends AbstractGenerator<Character> {

    private final char to, from;
    private final double nullFraction;

    public CharGenerator(char from, char to) {
        this.from = from;
        this.to = to;
        nullFraction = 0.0;
    }

    public CharGenerator(char from, char to, double nullFraction) {
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
