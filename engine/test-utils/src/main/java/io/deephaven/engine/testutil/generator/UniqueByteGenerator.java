package io.deephaven.engine.testutil.generator;

import java.util.Random;
import java.util.TreeMap;

public class UniqueByteGenerator extends AbstractUniqueGenerator<Byte> {

    private final byte to, from;
    private final double nullFraction;

    public UniqueByteGenerator(byte from, byte to) {
        this(from, to, 0.0);
    }

    public UniqueByteGenerator(byte from, byte to, double nullFraction) {
        this.from = from;
        this.to = to;
        this.nullFraction = nullFraction;
    }

    @Override
    public Byte nextValue(TreeMap<Long, Byte> values, long key, Random random) {
        if (nullFraction > 0) {
            if (random.nextDouble() < nullFraction) {
                return null;
            }
        }

        return (byte) (from + random.nextInt(to - from));
    }

    @Override
    public Class<Byte> getType() {
        return Byte.class;
    }
}
