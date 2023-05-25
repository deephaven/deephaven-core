package io.deephaven.engine.testutil.generator;

import io.deephaven.util.QueryConstants;

import java.util.Random;

public class PrimitiveGeneratorFunctions {
    static char generateChar(Random random, char from, char to) {
        return (char) (from + random.nextInt(to - from));
    }

    static byte generateByte(Random random, byte from, byte to) {
        return (byte) (from + random.nextInt(to - from));
    }

    static short generateShort(Random random, short from, short to) {
        return (short) (from + random.nextInt(to - from));
    }

    static int generateInt(Random random, int from, int to) {
        if (from == to) {
            return from;
        }
        final int bound = to - from;
        return (from + random.nextInt(bound));
    }

    public static long generateLong(Random random, long from, long to) {
        final long range = to - from;
        if (range > 0 && range < Integer.MAX_VALUE) {
            return from + random.nextInt(Math.toIntExact(range));
        } else if (from == QueryConstants.NULL_LONG + 1 && to == Long.MAX_VALUE) {
            long r;
            do {
                r = random.nextLong();
            } while (r == QueryConstants.NULL_LONG);
            return r;
        } else {
            final int bits = 64 - Long.numberOfLeadingZeros(range);
            long candidate = getRandomLong(random, bits);
            while (candidate > range || candidate < 0) {
                candidate = getRandomLong(random, bits);
            }
            return from + candidate;
        }
    }

    private static long getRandomLong(Random random, int bits) {
        final long value = random.nextLong();
        return bits >= 64 ? value : ((1L << bits) - 1L) & value;
    }

    static char minChar() {
        return 0;
    }

    static char maxChar() {
        return QueryConstants.NULL_CHAR - 1;
    }

    static byte minByte() {
        return QueryConstants.NULL_BYTE + 1;
    }

    static byte maxByte() {
        return Byte.MAX_VALUE;
    }

    static short minShort() {
        return QueryConstants.NULL_SHORT + 1;
    }

    static short maxShort() {
        return Short.MAX_VALUE;
    }

    static int minInt() {
        return Integer.MIN_VALUE / 2;
    }

    static int maxInt() {
        return Integer.MAX_VALUE / 2;
    }

    static long minLong() {
        return QueryConstants.NULL_LONG + 1;
    }

    static long maxLong() {
        return Long.MAX_VALUE;
    }

    public static double generateDouble(Random random, double from, double to) {
        return from + (random.nextDouble() * (to - from));
    }
}
