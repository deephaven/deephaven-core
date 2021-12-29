/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.function;

import java.util.concurrent.ThreadLocalRandom;

/**
 * A set of commonly used functions which are not associated with a specific type.
 */
public class SpecialPrimitives {

    /**
     * Returns a uniform random number between {@code 0.0} and {@code 1.0}.
     *
     * @return uniform random number between {@code 0.0} and {@code 1.0}.
     */
    static public double random() {
        return ThreadLocalRandom.current().nextDouble();
    }

    /**
     * Returns a random boolean.
     *
     * @return random boolean.
     */
    static public boolean randomBool() {
        return ThreadLocalRandom.current().nextBoolean();
    }

    /**
     * Returns an array of random booleans.
     *
     * @param size array size to generate.
     * @return array of random booleans.
     */
    static public boolean[] randomBool(int size) {
        final boolean[] result = new boolean[size];

        for (int i = 0; i < size; i++) {
            result[i] = randomBool();
        }

        return result;
    }

    /**
     * Returns a uniform random number.
     *
     * @param min minimum result.
     * @param max maximum result (exclusive).
     * @return uniform random number between {@code min} and {@code max} (exclusive).
     */
    static public int randomInt(int min, int max) {
        return min + ThreadLocalRandom.current().nextInt(max - min);
    }

    /**
     * Returns an array of uniform random numbers.
     *
     * @param min minimum result.
     * @param max maximum result (exclusive).
     * @param size array size to generate.
     * @return array of uniform random numbers between {@code min} and {@code max} (exclusive).
     */
    static public int[] randomInt(int min, int max, int size) {
        final int[] result = new int[size];

        for (int i = 0; i < size; i++) {
            result[i] = randomInt(min, max);
        }

        return result;
    }

    /**
     * Returns a uniform random number.
     *
     * @param min minimum result.
     * @param max maximum result (exclusive).
     * @return uniform random number between {@code min} and {@code max} (exclusive).
     */
    static public long randomLong(long min, long max) {
        return min + (long) (ThreadLocalRandom.current().nextDouble() * (max - min));
    }

    /**
     * Returns an array of uniform random numbers.
     *
     * @param min minimum result.
     * @param max maximum result (exclusive).
     * @param size array size to generate.
     * @return array of uniform random numbers between {@code min} and {@code max} (exclusive).
     */
    static public long[] randomLong(long min, long max, int size) {
        final long[] result = new long[size];

        for (int i = 0; i < size; i++) {
            result[i] = randomLong(min, max);
        }

        return result;
    }

    /**
     * Returns a uniform random number.
     *
     * @param min minimum result.
     * @param max maximum result.
     * @return uniform random number between {@code min} and {@code max}.
     */
    static public float randomFloat(float min, float max) {
        return min + (max - min) * ThreadLocalRandom.current().nextFloat();
    }

    /**
     * Returns an array of uniform random numbers.
     *
     * @param min minimum result.
     * @param max maximum result.
     * @param size array size to generate.
     * @return array of uniform random numbers between {@code min} and {@code max}.
     */
    static public float[] randomFloat(float min, float max, int size) {
        final float[] result = new float[size];

        for (int i = 0; i < size; i++) {
            result[i] = randomFloat(min, max);
        }

        return result;
    }

    /**
     * Returns a uniform random number.
     *
     * @param min minimum result.
     * @param max maximum result.
     * @return uniform random number between {@code min} and {@code max}.
     */
    static public double randomDouble(double min, double max) {
        return min + (max - min) * ThreadLocalRandom.current().nextDouble();
    }

    /**
     * Returns an array of uniform random numbers.
     *
     * @param min minimum result.
     * @param max maximum result.
     * @param size array size to generate.
     * @return array of uniform random numbers between {@code min} and {@code max}.
     */
    static public double[] randomDouble(double min, double max, int size) {
        final double[] result = new double[size];

        for (int i = 0; i < size; i++) {
            result[i] = randomDouble(min, max);
        }

        return result;
    }

    /**
     * Returns a Gaussian random number.
     *
     * @param mean mean.
     * @param std standard deviation.
     * @return Gaussian random number.
     */
    static public double randomGaussian(double mean, double std) {
        return mean + std * ThreadLocalRandom.current().nextGaussian();
    }

    /**
     * Returns an array of Gaussian random numbers.
     *
     * @param mean mean.
     * @param std standard deviation.
     * @param size array size to generate.
     * @return array of Gaussian random numbers.
     */
    static public double[] randomGaussian(double mean, double std, int size) {
        final double[] result = new double[size];

        for (int i = 0; i < size; i++) {
            result[i] = randomGaussian(mean, std);
        }

        return result;
    }
}
