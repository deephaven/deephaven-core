package io.deephaven.chunk;

import java.util.Random;

public class ReplicatorHelpers {
    public static boolean[] randomBooleans(Random rng, int size) {
        final boolean[] result = new boolean[size];
        for (int ii = 0; ii < size; ++ii) {
            result[ii] = rng.nextBoolean();
        }
        return result;
    }

    public static byte[] randomBytes(Random rng, int size) {
        return ArrayGenerator.randomBytes(rng, size);
    }

    public static char[] randomChars(Random rng, int size) {
        return ArrayGenerator.randomChars(rng, size);
    }

    public static double[] randomDoubles(Random rng, int size) {
        return ArrayGenerator.randomDoubles(rng, size);
    }

    public static float[] randomFloats(Random rng, int size) {
        return ArrayGenerator.randomFloats(rng, size);
    }

    public static int[] randomInts(Random rng, int size) {
        return ArrayGenerator.randomInts(rng, size);
    }

    public static long[] randomLongs(Random rng, int size) {
        return ArrayGenerator.randomLongs(rng, size);
    }

    public static short[] randomShorts(Random rng, int size) {
        return ArrayGenerator.randomShorts(rng, size);
    }

    public static Object[] randomObjects(Random rng, int size) {
        return ArrayGenerator.randomObjects(rng, size);
    }
}
