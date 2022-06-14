/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.function;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Functions for the generation random numbers.
 */
public class Random {

    /**
     * Returns a uniform random number between {@code 0.0} (inclusive) and {@code 1.0} (exclusive).
     *
     * @return uniform random number between {@code 0.0} (inclusive) and {@code 1.0} (exclusive).
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
     * @return uniform random number between {@code min} (inclusive) and {@code max} (exclusive).
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
     * @return array of uniform random numbers between {@code min} (inclusive) and {@code max} (exclusive).
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
     * @return uniform random number between {@code min} (inclusive) and {@code max} (exclusive).
     */
    static public long randomLong(long min, long max) {
        return ThreadLocalRandom.current().nextLong(min, max);
    }

    /**
     * Returns an array of uniform random numbers.
     *
     * @param min minimum result.
     * @param max maximum result (exclusive).
     * @param size array size to generate.
     * @return array of uniform random numbers between {@code min} (inclusive) and {@code max} (exclusive).
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
     * @return uniform random number between {@code min} (inclusive) and {@code max} (exclusive).
     */
    static public float randomFloat(float min, float max) {
        return (float) ThreadLocalRandom.current().nextDouble(min, max);
    }

    /**
     * Returns an array of uniform random numbers.
     *
     * @param min minimum result.
     * @param max maximum result.
     * @param size array size to generate.
     * @return array of uniform random numbers between {@code min} (inclusive) and {@code max} (exclusive).
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
     * @return uniform random number between {@code min} (inclusive) and {@code max} (exclusive).
     */
    static public double randomDouble(double min, double max) {
        return ThreadLocalRandom.current().nextDouble(min, max);
    }

    /**
     * Returns an array of uniform random numbers.
     *
     * @param min minimum result.
     * @param max maximum result.
     * @param size array size to generate.
     * @return array of uniform random numbers between {@code min} (inclusive) and {@code max} (exclusive).
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
