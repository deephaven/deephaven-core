package io.deephaven.engine.testutil.generator;

import io.deephaven.util.QueryConstants;

import java.util.Random;
import java.util.TreeMap;

public class ByteGenerator extends AbstractGenerator<Byte> {

    private final byte to, from;
    private final double nullFraction;

    public ByteGenerator() {
        this((byte) (QueryConstants.NULL_BYTE + 1), Byte.MAX_VALUE);
    }

    public ByteGenerator(byte from, byte to) {
        this(from, to, 0);
    }

    public ByteGenerator(byte from, byte to, double nullFraction) {
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
